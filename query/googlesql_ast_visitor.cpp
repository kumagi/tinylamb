/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/googlesql_ast_visitor.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <iomanip>
#include <limits>
#include <memory>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/googlesql_ast.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// Renders an array Value as a compact JSON array (used by struct literals so
// nested containers keep their shape instead of the ARRAY<T>[...] spelling).
std::string ValueToJsonArray(const Value& value);

int ParseTimeZoneOffset(std::string_view tz_str, int Y, int M, int D, int h,
                        int m, int s, int default_offset = 0) {
  if (tz_str.empty()) {
    return default_offset;
  }
  if (tz_str == "UTC" || tz_str == "GMT" || tz_str == "utc" ||
      tz_str == "gmt" || tz_str == "Z" || tz_str == "z" ||
      tz_str == "Etc/Greenwich" || tz_str == "Etc/UTC" || tz_str == "Etc/GMT") {
    return 0;
  }
  if (tz_str.starts_with("UTC+") || tz_str.starts_with("UTC-") ||
      tz_str.starts_with("GMT+") || tz_str.starts_with("GMT-")) {
    char sign = tz_str[3];
    int th = 0, tm = 0;
    std::string rem(tz_str.substr(4));
    if (rem.find(':') != std::string::npos) {
      sscanf(rem.c_str(), "%d:%d", &th, &tm);
    } else if (rem.size() == 4) {
      sscanf(rem.c_str(), "%2d%2d", &th, &tm);
    } else {
      sscanf(rem.c_str(), "%d", &th);
    }
    return (th * 3600 + tm * 60) * (sign == '-' ? -1 : 1);
  }
  if (tz_str[0] == '+' || tz_str[0] == '-') {
    char sign = tz_str[0];
    int th = 0, tm = 0;
    std::string rem(tz_str.substr(1));
    if (rem.find(':') != std::string::npos) {
      sscanf(rem.c_str(), "%d:%d", &th, &tm);
    } else if (rem.size() == 4) {
      sscanf(rem.c_str(), "%2d%2d", &th, &tm);
    } else {
      sscanf(rem.c_str(), "%d", &th);
    }
    return (th * 3600 + tm * 60) * (sign == '-' ? -1 : 1);
  }
  std::string zone_name(tz_str);
  if (zone_name == "NZ-CHAT") {
    zone_name = "Pacific/Chatham";
  }
  try {
    const auto* zone = std::chrono::locate_zone(zone_name);
    if (zone) {
      int y = Y < 1970 ? 1970 : Y;
      std::chrono::year_month_day ymd{
          std::chrono::year{y}, std::chrono::month{static_cast<unsigned>(M)},
          std::chrono::day{static_cast<unsigned>(D)}};
      std::chrono::local_days loc_d{ymd};
      auto loc_tp = loc_d + std::chrono::hours{h} + std::chrono::minutes{m} +
                    std::chrono::seconds{s};
      auto loc_info = zone->get_info(loc_tp);
      return static_cast<int>(loc_info.first.offset.count());
    }
  } catch (...) {
  }
  return default_offset;
}

// Boolean literals may be spelled in any case; GoogleSQL keywords are
// case-insensitive.
inline bool IsBooleanLiteralDetail(const std::string& detail, bool want_true) {
  std::string upper;
  upper.reserve(detail.size());
  for (char c : detail) {
    upper.push_back(
        static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return want_true ? (upper == "TRUE") : (upper == "FALSE");
}

std::string SqlTypeFromAst(const GoogleSqlAstNode& node);
std::string ValueToJsonArray(const Value& value);

std::string Lower(std::string value) {
  std::ranges::transform(value, value.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return value;
}

// Deeply nested expressions (e.g. "1+1+1+..." chained 100k times) parse fine
// but would overflow the C++ stack during recursive visitation; a stack
// overflow is unrecoverable, so cap the visitation depth explicitly.
constexpr size_t kMaxExpressionDepth = 512;

size_t& ExpressionDepthCounter() {
  static thread_local size_t depth = 0;
  return depth;
}

class ExpressionDepthGuard {
 public:
  ExpressionDepthGuard() {
    size_t& depth = ExpressionDepthCounter();
    if (depth >= kMaxExpressionDepth) {
      throw std::runtime_error("GoogleSQL AST: expression nesting exceeds " +
                               std::to_string(kMaxExpressionDepth));
    }
    ++depth;
  }
  ~ExpressionDepthGuard() { --ExpressionDepthCounter(); }
  ExpressionDepthGuard(const ExpressionDepthGuard&) = delete;
  ExpressionDepthGuard& operator=(const ExpressionDepthGuard&) = delete;
  ExpressionDepthGuard(ExpressionDepthGuard&&) = delete;
  ExpressionDepthGuard& operator=(ExpressionDepthGuard&&) = delete;
};

// Dump-produced literals must be digits-only and in range: std::stoll would
// accept signs and std::stoull would wrap "-1" into a huge positive value.
int64_t ParseIntLiteral(const GoogleSqlAstNode& node) {
  const std::string& text = node.detail;
  const bool digits_only =
      !text.empty() &&
      std::ranges::all_of(text, [](char c) { return '0' <= c && c <= '9'; });
  if (!digits_only) {
    throw std::runtime_error("GoogleSQL AST: malformed integer literal " +
                             text);
  }
  if (text == "9223372036854775808") {
    return std::numeric_limits<int64_t>::min();
  }
  int64_t value = 0;
  const auto* end = text.data() + text.size();
  const auto [ptr, ec] = std::from_chars(text.data(), end, value);
  if (ec != std::errc() || ptr != end) {
    throw std::runtime_error("GoogleSQL AST: integer literal out of range " +
                             text);
  }
  return value;
}

uint64_t ParseUnsignedLiteral(const GoogleSqlAstNode& node) {
  const std::string& text = node.detail;
  const bool digits_only =
      !text.empty() &&
      std::ranges::all_of(text, [](char c) { return '0' <= c && c <= '9'; });
  if (!digits_only) {
    throw std::runtime_error("GoogleSQL AST: malformed unsigned literal " +
                             text);
  }
  uint64_t value = 0;
  const auto* end = text.data() + text.size();
  const auto [ptr, ec] = std::from_chars(text.data(), end, value);
  if (ec != std::errc() || ptr != end) {
    throw std::runtime_error("GoogleSQL AST: unsigned literal out of range " +
                             text);
  }
  return value;
}

double ParseFloatLiteral(const GoogleSqlAstNode& node) {
  try {
    return std::stod(node.detail);
  } catch (const std::exception&) {
    throw std::runtime_error("GoogleSQL AST: float literal out of range " +
                             node.detail);
  }
}

std::string DecodeSingleComponent(std::string_view value_view);

std::string Identifier(const GoogleSqlAstNode& node) {
  if (node.kind != "Identifier") {
    throw std::runtime_error("GoogleSQL AST: expected Identifier");
  }
  std::string value = node.detail;
  if (value.size() >= 2 && value.front() == '`' && value.back() == '`') {
    value =
        DecodeSingleComponent("\"" + value.substr(1, value.size() - 2) + "\"");
  }
  return value;
}

std::vector<std::string> PathParts(const GoogleSqlAstNode& path) {
  std::vector<std::string> result;
  for (const auto& child : path.children) {
    if (child->kind == "Identifier") {
      result.push_back(Identifier(*child));
    }
  }
  if (result.empty()) {
    throw std::runtime_error("GoogleSQL AST: empty path expression");
  }
  return result;
}

std::string Path(const GoogleSqlAstNode& path) {
  const std::vector<std::string> parts = PathParts(path);
  std::string result;
  for (const std::string& part : parts) {
    if (!result.empty()) {
      result += '.';
    }
    result += part;
  }
  return result;
}

std::string Alias(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* alias = node.Child("Alias");
  if (alias == nullptr || alias->Child("Identifier") == nullptr) {
    return {};
  }
  return Identifier(*alias->Child("Identifier"));
}

std::string DecodeSingleComponent(std::string_view value_view) {
  std::string value = std::string(value_view);
  bool is_raw = false;
  bool is_bytes = false;
  if (value.size() >= 1 && (value.front() == 'r' || value.front() == 'R')) {
    is_raw = true;
    value = value.substr(1);
  }
  if (value.size() >= 1 && (value.front() == 'b' || value.front() == 'B')) {
    is_bytes = true;
    value = value.substr(1);
  }
  if (!is_raw && value.size() >= 1 &&
      (value.front() == 'r' || value.front() == 'R')) {
    is_raw = true;
    value = value.substr(1);
  }

  bool is_triple = false;
  char quote = '\0';
  if (value.size() >= 6 &&
      ((value.starts_with("\"\"\"") && value.ends_with("\"\"\"")) ||
       (value.starts_with("'''") && value.ends_with("'''")))) {
    is_triple = true;
    quote = value.front();
    value = value.substr(3, value.size() - 6);
  } else if (value.size() >= 2 &&
             ((value.front() == '\'' && value.back() == '\'') ||
              (value.front() == '"' && value.back() == '"'))) {
    quote = value.front();
    value = value.substr(1, value.size() - 2);
  } else {
    return value;
  }

  if (is_raw) {
    return value;
  }

  std::string decoded;
  decoded.reserve(value.size());
  for (size_t i = 0; i < value.size(); ++i) {
    if (value[i] == '\\' && i + 1 < value.size()) {
      char next = value[++i];
      if (next == 'a') {
        decoded.push_back('\a');
      } else if (next == 'b') {
        decoded.push_back('\b');
      } else if (next == 'f') {
        decoded.push_back('\f');
      } else if (next == 'n') {
        decoded.push_back('\n');
      } else if (next == 'r') {
        decoded.push_back('\r');
      } else if (next == 't') {
        decoded.push_back('\t');
      } else if (next == 'v') {
        decoded.push_back('\v');
      } else if (next == '\\') {
        decoded.push_back('\\');
      } else if (next == '\'') {
        decoded.push_back('\'');
      } else if (next == '"') {
        decoded.push_back('"');
      } else if (next == '`') {
        decoded.push_back('`');
      } else if (next == '?' || next == '/') {
        decoded.push_back(next);
      } else if (next >= '0' && next <= '7') {
        std::string oct_str;
        oct_str.push_back(next);
        if (i + 1 < value.size() && value[i + 1] >= '0' &&
            value[i + 1] <= '7') {
          oct_str.push_back(value[++i]);
          if (i + 1 < value.size() && value[i + 1] >= '0' &&
              value[i + 1] <= '7') {
            oct_str.push_back(value[++i]);
          }
        }
        try {
          uint32_t raw_byte = std::stoul(oct_str, nullptr, 8);
          if (is_bytes || raw_byte <= 0x7F) {
            decoded.push_back(static_cast<char>(raw_byte));
          } else {
            decoded.push_back(
                static_cast<char>(0xC0 | ((raw_byte >> 6) & 0x1F)));
            decoded.push_back(static_cast<char>(0x80 | (raw_byte & 0x3F)));
          }
        } catch (...) {
          decoded.push_back(next);
        }
      } else if (next == 'x' || next == 'X') {
        if (i + 2 < value.size()) {
          std::string hex_str = value.substr(i + 1, 2);
          try {
            uint32_t raw_byte = std::stoul(hex_str, nullptr, 16);
            if (is_bytes || raw_byte <= 0x7F) {
              decoded.push_back(static_cast<char>(raw_byte));
            } else {
              decoded.push_back(
                  static_cast<char>(0xC0 | ((raw_byte >> 6) & 0x1F)));
              decoded.push_back(static_cast<char>(0x80 | (raw_byte & 0x3F)));
            }
            i += 2;
          } catch (...) {
            decoded.push_back(next);
          }
        } else {
          decoded.push_back(next);
        }
      } else if (next == 'u') {
        if (i + 4 < value.size()) {
          std::string hex_str = value.substr(i + 1, 4);
          try {
            uint32_t cp = std::stoul(hex_str, nullptr, 16);
            if (cp <= 0x7F) {
              decoded.push_back(static_cast<char>(cp));
            } else if (cp <= 0x7FF) {
              decoded.push_back(static_cast<char>(0xC0 | ((cp >> 6) & 0x1F)));
              decoded.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else if (cp <= 0xFFFF) {
              decoded.push_back(static_cast<char>(0xE0 | ((cp >> 12) & 0x0F)));
              decoded.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
              decoded.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else {
              decoded.push_back(static_cast<char>(0xF0 | ((cp >> 18) & 0x07)));
              decoded.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
              decoded.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
              decoded.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            }
            i += 4;
          } catch (...) {
            decoded.push_back(next);
          }
        } else {
          decoded.push_back(next);
        }
      } else if (next == 'U') {
        if (i + 8 < value.size()) {
          std::string hex_str = value.substr(i + 1, 8);
          try {
            uint32_t cp = std::stoul(hex_str, nullptr, 16);
            if (cp <= 0x7F) {
              decoded.push_back(static_cast<char>(cp));
            } else if (cp <= 0x7FF) {
              decoded.push_back(static_cast<char>(0xC0 | ((cp >> 6) & 0x1F)));
              decoded.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else if (cp <= 0xFFFF) {
              decoded.push_back(static_cast<char>(0xE0 | ((cp >> 12) & 0x0F)));
              decoded.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
              decoded.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else {
              decoded.push_back(static_cast<char>(0xF0 | ((cp >> 18) & 0x07)));
              decoded.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
              decoded.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
              decoded.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            }
            i += 8;
          } catch (...) {
            decoded.push_back(next);
          }
        } else {
          decoded.push_back(next);
        }
      } else {
        decoded.push_back(next);
      }
    } else if (!is_triple && value[i] == quote && i + 1 < value.size() &&
               value[i + 1] == quote) {
      decoded.push_back(quote);
      ++i;
    } else {
      decoded.push_back(value[i]);
    }
  }
  return decoded;
}

std::string DecodeString(const GoogleSqlAstNode& node) {
  std::string result;
  bool found_component = false;
  for (const auto& child : node.children) {
    if (child->kind == "StringLiteralComponent") {
      result += DecodeSingleComponent(child->detail);
      found_component = true;
    }
  }
  if (found_component) {
    return result;
  }
  return DecodeSingleComponent(node.detail);
}

BinaryOperation BinaryOp(std::string_view detail) {
  if (detail == "+") {
    return BinaryOperation::kAdd;
  }
  if (detail == "-") {
    return BinaryOperation::kSubtract;
  }
  if (detail == "*") {
    return BinaryOperation::kMultiply;
  }
  if (detail == "/") {
    return BinaryOperation::kDivide;
  }
  if (detail == "%") {
    return BinaryOperation::kModulo;
  }
  if (detail == "=") {
    return BinaryOperation::kEquals;
  }
  if (detail == "!=" || detail == "<>") {
    return BinaryOperation::kNotEquals;
  }
  if (detail == "<") {
    return BinaryOperation::kLessThan;
  }
  if (detail == "<=") {
    return BinaryOperation::kLessThanEquals;
  }
  if (detail == ">") {
    return BinaryOperation::kGreaterThan;
  }
  if (detail == ">=") {
    return BinaryOperation::kGreaterThanEquals;
  }
  if (detail == "LIKE") {
    return BinaryOperation::kLike;
  }
  if (detail == "NOT LIKE") {
    return BinaryOperation::kNotLike;
  }
  throw std::runtime_error("GoogleSQL AST: unsupported binary operator " +
                           std::string(detail));
}

// True when the expression's top-level scalar is wrapped by an explicit
// COLLATE(...) call (possibly under struct field access).  Such comparisons
// must honor the collation (case folding for 'und:ci') instead of raw binary
// string comparison, which the plain Value operators cannot express.
bool HasTopLevelCollate(
    const Expression& expression) {  // NOLINT(misc-no-recursion)
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp) {
    const auto& call = expression->AsFunctionCallExpression();
    std::string name = call.FuncName();
    for (char& c : name) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (name == "collate") {
      return true;
    }
    if (name == "get_field" && !call.Args().empty()) {
      return HasTopLevelCollate(call.Args()[0]);
    }
  }
  return false;
}

// Comparisons whose either side carries an explicit COLLATE() wrapper are
// lowered to __cmp_ci(left, right, op) so the evaluator can apply case
// folding.  Everything else keeps the regular binary operator.
Expression MakeComparison(Expression left, std::string_view op_text,
                          Expression right) {
  if (HasTopLevelCollate(left) || HasTopLevelCollate(right)) {
    return FunctionCallExp("__cmp_ci",
                           {std::move(left), std::move(right),
                            ConstantValueExp(Value(std::string(op_text)))});
  }
  return BinaryExpressionExp(std::move(left), BinaryOp(op_text),
                             std::move(right));
}

std::shared_ptr<SelectStatement> VisitQuery(const GoogleSqlAstNode& query);
Expression VisitExpression(const GoogleSqlAstNode& node);

bool NeedsRelationalEvaluation(
    const Expression&
        expression,  // NOLINT(misc-no-recursion) // AST traversal recursion is
                     // intentional; expression depth bounded by
                     // ExpressionDepthGuard (kMaxExpressionDepth).
    bool top_level = true) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kQueryExp:
    case TypeTag::kIntervalExp:
      return true;
    case TypeTag::kAggregateExp:
      return !top_level || NeedsRelationalEvaluation(
                               expression->AsAggregateExpression().Child());
    case TypeTag::kBinaryExp:
      // OR used to force the materializing relational executor because the
      // cost-based scan rules could not derive an access path for it.  They
      // now support both a full-scan residual and disjoint composite-prefix
      // index unions, so OR is no longer a complexity boundary.
      return NeedsRelationalEvaluation(expression->AsBinaryExpression().Left(),
                                       false) ||
             NeedsRelationalEvaluation(expression->AsBinaryExpression().Right(),
                                       false);
    case TypeTag::kUnaryExp:
      return NeedsRelationalEvaluation(expression->AsUnaryExpression().Child(),
                                       false);
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      for (const auto& [condition, result] : value.when_clauses_) {
        if (NeedsRelationalEvaluation(condition, false) ||
            NeedsRelationalEvaluation(result, false)) {
          return true;
        }
      }
      return NeedsRelationalEvaluation(value.else_clause_, false);
    }
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      if (NeedsRelationalEvaluation(value.child_, false)) {
        return true;
      }
      return std::ranges::any_of(
          value.list_, [](const Expression&
                              item) {  // NOLINT(misc-no-recursion) // Part of
                                       // NeedsRelationalEvaluation recursion;
                                       // depth-guarded by ExpressionDepthGuard.
            return NeedsRelationalEvaluation(item, false);
          });
    }
    case TypeTag::kFunctionCallExp: {
      return true;
    }
    case TypeTag::kWindowFunctionExp:
      // Window functions always evaluate through the relational engine's
      // hidden-column pre-computation.
      return true;
    case TypeTag::kArrayExp:
      return std::ranges::any_of(expression->AsArrayExpression().Elements(),
                                 [](const Expression& element) {
                                   return NeedsRelationalEvaluation(element,
                                                                    false);
                                 });
    default:
      return false;
  }
}

Expression FoldBoolean(
    const GoogleSqlAstNode& node,
    BinaryOperation op) {  // NOLINT(misc-no-recursion) // AST traversal
                           // recursion is intentional; depth bounded by
                           // ExpressionDepthGuard in VisitExpression.
  Expression result;
  for (const auto& child : node.children) {
    Expression next = VisitExpression(*child);
    result = result
                 ? BinaryExpressionExp(std::move(result), op, std::move(next))
                 : std::move(next);
  }
  if (!result) {
    throw std::runtime_error("GoogleSQL AST: empty boolean node");
  }
  return result;
}

bool IsBooleanAstNode(const GoogleSqlAstNode& node) {
  if (node.kind == "BooleanLiteral") {
    return true;
  }
  if (node.kind == "BinaryExpression") {
    if (node.detail == "=" || node.detail == "!=" || node.detail == "<>" ||
        node.detail == "<" || node.detail == "<=" || node.detail == ">" ||
        node.detail == ">=" || node.detail == "LIKE" ||
        node.detail == "NOT LIKE" || node.detail == "IS" ||
        node.detail == "IS NOT") {
      return true;
    }
  }
  if (node.kind == "UnaryExpression" && node.detail == "NOT") {
    return true;
  }
  if (node.kind == "AndExpr" || node.kind == "OrExpr") {
    return true;
  }
  if (node.kind == "CastExpression" && node.children.size() >= 2) {
    const std::string t = SqlTypeFromAst(*node.children[1]);
    return t == "BOOL" || t == "BOOLEAN";
  }
  return false;
}

Expression WrapIfBoolean(Expression expr, const GoogleSqlAstNode& node) {
  if (IsBooleanAstNode(node)) {
    return CaseExpressionExp(
        {{std::move(expr), ConstantValueExp(Value(std::string("true")))}},
        ConstantValueExp(Value(std::string("false"))));
  }
  return expr;
}

std::string UpperCopy(std::string text) {
  for (char& c : text) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return text;
}

bool IsBytesAstNode(const GoogleSqlAstNode& node) {
  if (node.kind == "BytesLiteral") {
    return true;
  }
  if (node.kind == "CastExpression" && node.children.size() >= 2) {
    const GoogleSqlAstNode& type_node = *node.children[1];
    std::string type_name = SqlTypeFromAst(type_node);
    if (type_name.empty()) {
      if (const auto* path = type_node.Child("PathExpression")) {
        type_name = Path(*path);
      } else if (type_node.kind == "SimpleType") {
        for (const auto& c : type_node.children) {
          type_name = SqlTypeFromAst(*c);
          if (!type_name.empty()) {
            break;
          }
        }
      }
    }
    if (UpperCopy(type_name).find("BYTES") != std::string::npos ||
        type_node.detail.find("BYTES") != std::string::npos) {
      return true;
    }
  }
  if (node.kind == "FunctionCall") {
    if (!node.children.empty() &&
        node.children.front()->kind == "PathExpression") {
      const std::string fn = Lower(Path(*node.children.front()));
      if (fn == "byte_substr" || fn == "b") {
        return true;
      }
    }
  }
  return false;
}

WindowOrderTerm ParseOrderingTerm(const GoogleSqlAstNode* term) {
  WindowOrderTerm parsed;
  if (term == nullptr || term->children.empty()) {
    return parsed;
  }
  const GoogleSqlAstNode* collate_clause = nullptr;
  for (const auto& child : term->children) {
    if (child->kind == "Location") {
      continue;
    }
    if (child->kind == "Collate") {
      collate_clause = child.get();
      continue;
    }
    if (child->kind == "NullOrder") {
      parsed.nulls_first =
          UpperCopy(child->detail).find("NULLS FIRST") != std::string::npos;
      continue;
    }
    if (!parsed.expression) {
      parsed.expression = VisitExpression(*child);
    }
  }
  if (collate_clause != nullptr && parsed.expression) {
    // Hidden sort keys carry the collation as a collate() wrapper so the
    // window/sort comparators can fold case.
    std::string collation;
    for (const auto& inner : collate_clause->children) {
      if (inner->kind == "StringLiteral") {
        collation = DecodeString(*inner);
        break;
      }
    }
    parsed.expression = FunctionCallExp(
        "collate", {std::move(parsed.expression),
                    ConstantValueExp(Value(std::move(collation)))});
  }
  parsed.ascending = UpperCopy(term->detail) != "DESC";
  return parsed;
}

std::vector<WindowOrderTerm> ParseOrderingList(const GoogleSqlAstNode& order) {
  std::vector<WindowOrderTerm> terms;
  for (const GoogleSqlAstNode* term : order.Children("OrderingExpression")) {
    WindowOrderTerm parsed = ParseOrderingTerm(term);
    if (parsed.expression) {
      terms.push_back(std::move(parsed));
    }
  }
  return terms;
}

Expression VisitFunction(
    const GoogleSqlAstNode&
        node) {  // NOLINT(misc-no-recursion) // AST traversal recursion is
                 // intentional; depth bounded by ExpressionDepthGuard in
                 // VisitExpression.
  if (node.children.empty() ||
      node.children.front()->kind != "PathExpression") {
    throw std::runtime_error("GoogleSQL AST: function without name");
  }
  std::string name = Lower(Path(*node.children.front()));
  // SAFE.<fn>(...) lowers to __safe(<fn>(...)): evaluation errors become NULL.
  const bool is_safe_call = name.starts_with("safe.") && name.size() > 5;
  if (is_safe_call) {
    name = name.substr(5);
  }
  if (name == "ucase") {
    name = "upper";
  }
  if (name == "lcase") {
    name = "lower";
  }
  const bool first_arg_bytes =
      node.children.size() > 1 && IsBytesAstNode(*node.children[1]);
  if (first_arg_bytes) {
    if (name == "substr" || name == "substring") {
      name = "byte_substr";
    } else if (name == "length" || name == "char_length" ||
               name == "character_length") {
      name = "byte_length";
    } else if (name == "reverse") {
      name = "byte_reverse";
    }
  }
  std::vector<Expression> arguments;
  AggregateHavingModifier having = AggregateHavingModifier::kNone;
  Expression having_condition;
  Expression where_filter;
  std::vector<WindowOrderTerm> inner_order_by;
  std::optional<size_t> inner_limit;

  for (size_t i = 1; i < node.children.size(); ++i) {
    const GoogleSqlAstNode& child = *node.children[i];
    if (child.kind == "Location") {
      continue;
    }
    if (child.kind == "WhereClause") {
      // AGG(x WHERE cond): row-level pre-filter before aggregation.
      if (!child.children.empty()) {
        where_filter = VisitExpression(*child.children[0]);
      }
      continue;
    }
    if (child.kind == "HavingModifier") {
      // AGG(x HAVING MAX cond): detail carries MAX or MIN.
      const std::string modifier = UpperCopy(child.detail);
      having = modifier.find("MIN") != std::string::npos
                   ? AggregateHavingModifier::kMin
                   : AggregateHavingModifier::kMax;
      for (const auto& grandchild : child.children) {
        if (grandchild->kind != "Location") {
          having_condition = VisitExpression(*grandchild);
          break;
        }
      }
      continue;
    }
    if (child.kind == "OrderBy") {
      inner_order_by = ParseOrderingList(child);
      continue;
    }
    if (child.kind == "LimitOffset") {
      if (const GoogleSqlAstNode* limit_node = child.Child("Limit")) {
        if (const GoogleSqlAstNode* value = limit_node->Child("IntLiteral")) {
          inner_limit = static_cast<size_t>(ParseUnsignedLiteral(*value));
        }
      }
      continue;
    }
    Expression arg = VisitExpression(child);
    if (name == "concat") {
      arg = WrapIfBoolean(std::move(arg), child);
    }
    // ARRAY_ZIP carries its STRUCT field labels (`<array> AS name`) as
    // trailing metadata constants consumed by the evaluator.
    if (name == "array_zip") {
      std::string field_name;
      if (child.kind == "ExpressionWithAlias") {
        if (const GoogleSqlAstNode* alias = child.Child("Alias")) {
          if (const GoogleSqlAstNode* id = alias->Child("Identifier")) {
            field_name = Identifier(*id);
          }
        }
      } else if (child.kind == "NamedArgument") {
        // mode => 'PAD' keeps the raw text marker below.
        field_name = "\x02mode";
      }
      arguments.push_back(std::move(arg));
      arguments.push_back(ConstantValueExp(Value(std::move(field_name))));
      continue;
    }
    arguments.push_back(std::move(arg));
  }

  auto finish_aggregate = [&](AggregationType type) -> Expression {
    auto aggregate = std::make_shared<AggregateExpression>(
        type, arguments.empty() ? nullptr : arguments[0],
        node.detail.find("distinct=true") != std::string::npos);
    if (having != AggregateHavingModifier::kNone) {
      aggregate->SetHaving(having, having_condition);
    }
    if (where_filter) {
      aggregate->SetWhereFilter(where_filter);
    }
    if (!inner_order_by.empty()) {
      aggregate->SetInnerOrderBy(inner_order_by);
    }
    if (inner_limit.has_value()) {
      aggregate->SetInnerLimit(inner_limit);
    }
    if (type == AggregationType::kStringAgg && arguments.size() > 1) {
      aggregate->SetSecondaryArg(arguments[1]);
    }
    return aggregate;
  };

  if (name == "count" || name == "sum" || name == "avg" || name == "min" ||
      name == "max" || name == "logical_and" || name == "logical_or" ||
      name == "array_agg" || name == "string_agg" || name == "countif" ||
      name == "any_value" || name == "var_pop" || name == "var_samp" ||
      name == "stddev_pop" || name == "stddev_samp" || name == "variance" ||
      name == "stddev" || name == "covar_pop" || name == "covar_samp" ||
      name == "corr" || name == "bit_and" || name == "bit_or" ||
      name == "bit_xor" || name == "elementwise_sum" ||
      name == "elementwise_avg") {
    if ((name == "count" || name == "sum" || name == "avg" || name == "min" ||
         name == "max" || name == "logical_and" || name == "logical_or" ||
         name == "array_agg" || name == "countif" || name == "any_value" ||
         name == "var_pop" || name == "var_samp" || name == "stddev_pop" ||
         name == "stddev_samp" || name == "variance" || name == "stddev" ||
         name == "elementwise_sum" || name == "elementwise_avg") &&
        arguments.size() != 1 && !(name == "count" && !arguments.empty())) {
      throw std::runtime_error("GoogleSQL AST: aggregate arity");
    }
    AggregationType type = AggregationType::kCount;
    if (name == "sum") {
      type = AggregationType::kSum;
    }
    if (name == "avg") {
      type = AggregationType::kAvg;
    }
    if (name == "min") {
      type = AggregationType::kMin;
    }
    if (name == "max") {
      type = AggregationType::kMax;
    }
    if (name == "logical_and") {
      type = AggregationType::kLogicalAnd;
    }
    if (name == "logical_or") {
      type = AggregationType::kLogicalOr;
    }
    if (name == "array_agg") {
      type = AggregationType::kArrayAgg;
    }
    if (name == "string_agg") {
      type = AggregationType::kStringAgg;
    }
    if (name == "countif") {
      type = AggregationType::kCountIf;
    }
    if (name == "any_value") {
      type = AggregationType::kAnyValue;
    }
    if (name == "var_pop") {
      type = AggregationType::kVarPop;
    }
    if (name == "var_samp" || name == "variance") {
      type = AggregationType::kVarSamp;
    }
    if (name == "stddev_pop") {
      type = AggregationType::kStddevPop;
    }
    if (name == "stddev_samp" || name == "stddev") {
      type = AggregationType::kStddevSamp;
    }
    if (name == "covar_pop") {
      type = AggregationType::kCovarPop;
    }
    if (name == "covar_samp") {
      type = AggregationType::kCovarSamp;
    }
    if (name == "corr") {
      type = AggregationType::kCorr;
    }
    if (name == "bit_and") {
      type = AggregationType::kBitAnd;
    }
    if (name == "bit_or") {
      type = AggregationType::kBitOr;
    }
    if (name == "bit_xor") {
      type = AggregationType::kBitXor;
    }
    if (name == "elementwise_sum") {
      type = AggregationType::kElementwiseSum;
    }
    if (name == "elementwise_avg") {
      type = AggregationType::kElementwiseAvg;
    }
    auto aggregate = finish_aggregate(type);
    // Two-input statistical aggregates take y from the secondary argument.
    if (!arguments.empty() && arguments.size() > 1) {
      std::dynamic_pointer_cast<AggregateExpression>(aggregate)
          ->SetSecondaryArg(arguments[1]);
    }
    return aggregate;
  }
  Expression call = FunctionCallExp(name, std::move(arguments));
  if (is_safe_call) {
    call = FunctionCallExp("__safe", {std::move(call)});
  }
  return call;
}

bool IsAstTrivia(std::string_view kind) { return kind == "Location"; }

bool IsArrayTypeNode(std::string_view kind) { return kind == "ArrayType"; }

std::string SqlTypeFromAst(
    const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion)
  if (IsAstTrivia(node.kind)) {
    return {};
  }
  if (node.kind == "PathExpression") {
    return UpperCopy(Path(node));
  }
  if (node.kind == "Identifier") {
    return UpperCopy(node.detail);
  }
  if (node.kind == "ArrayType") {
    for (const auto& child : node.children) {
      const std::string nested = SqlTypeFromAst(*child);
      if (!nested.empty()) {
        return "ARRAY<" + nested + ">";
      }
    }
    return "ARRAY<INT64>";
  }
  for (const auto& child : node.children) {
    const std::string nested = SqlTypeFromAst(*child);
    if (!nested.empty()) {
      return nested;
    }
  }
  return {};
}

std::string InferArrayElementSqlType(
    const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion)
  if (node.kind == "BooleanLiteral") {
    return "BOOL";
  }
  if (node.kind == "FloatLiteral") {
    return "FLOAT64";
  }
  if (node.kind == "StringLiteral") {
    return "STRING";
  }
  if (node.kind == "BytesLiteral") {
    return "BYTES";
  }
  if (node.kind == "DateOrTimeLiteral") {
    if (node.detail == "TYPE_DATE") {
      return "DATE";
    }
    if (node.detail == "TYPE_TIMESTAMP") {
      return "TIMESTAMP";
    }
    if (node.detail == "TYPE_TIME") {
      return "TIME";
    }
    if (node.detail == "TYPE_DATETIME") {
      return "DATETIME";
    }
    return "STRING";
  }
  if (node.kind == "IntervalExpr") {
    return "INTERVAL";
  }
  if (node.kind == "CastExpression" && node.children.size() >= 2) {
    return SqlTypeFromAst(*node.children[1]);
  }
  if (node.kind == "NewConstructor") {
    return "PROTO";
  }
  if (node.kind == "NullLiteral") {
    return {};
  }

  if (node.kind == "ArrayConstructor") {
    std::string inner;
    for (const auto& child : node.children) {
      if (IsAstTrivia(child->kind)) {
        continue;
      }
      if (child->kind == "ArrayType") {
        inner = SqlTypeFromAst(*child);
        if (inner.starts_with("ARRAY<") && inner.back() == '>') {
          inner = inner.substr(6, inner.size() - 7);
        }
        break;
      }
      inner = InferArrayElementSqlType(*child);
      if (!inner.empty()) {
        break;
      }
    }
    if (inner.empty()) {
      inner = "INT64";
    }
    return "ARRAY<" + inner + ">";
  }
  return "INT64";
}

std::string DecodeBytes(const GoogleSqlAstNode& node) {
  std::string result;
  bool found_component = false;
  for (const auto& child : node.children) {
    if (child->kind == "BytesLiteralComponent") {
      result += DecodeSingleComponent(child->detail);
      found_component = true;
    }
  }
  if (found_component) {
    return result;
  }
  return DecodeSingleComponent(node.detail);
}

// ---------------------------------------------------------------------------
// Analytic (window) functions: `f(x) OVER spec` plus the WINDOW clause that
// names reusable specifications.  The visitor resolves named references into
// fully-expanded WindowFunctionCallExpression nodes so the executor only sees
// concrete partition/order/frame definitions.
// ---------------------------------------------------------------------------

struct NamedWindowParts {
  std::vector<Expression> partition_by;
  std::vector<WindowOrderTerm> order_by;
  WindowFrameUnit frame_unit{WindowFrameUnit::kDefault};
  WindowFrameBound frame_start;
  WindowFrameBound frame_end;
  bool has_frame{false};
  WindowFrameExclusion exclusion{WindowFrameExclusion::kNone};
};

thread_local std::unordered_map<std::string, NamedWindowParts> t_named_windows;

// Parses one OrderingExpression: children[0] is the key expression and an
// optional NullOrder child carries an explicit NULLS FIRST / NULLS LAST.

WindowFrameBound ParseFrameBound(const GoogleSqlAstNode& node) {
  // detail is one of "UNBOUNDED PRECEDING", "OFFSET PRECEDING",
  // "CURRENT ROW", "OFFSET FOLLOWING", "UNBOUNDED FOLLOWING".
  const std::string text = UpperCopy(node.detail);
  WindowFrameBound bound;
  if (text.find("CURRENT ROW") != std::string::npos) {
    bound.type = WindowFrameBoundType::kCurrentRow;
    return bound;
  }
  const bool preceding = text.find("PRECEDING") != std::string::npos;
  const bool unbounded = text.find("UNBOUNDED") != std::string::npos;
  if (unbounded) {
    bound.type = preceding ? WindowFrameBoundType::kUnboundedPreceding
                           : WindowFrameBoundType::kUnboundedFollowing;
    return bound;
  }
  bound.type = preceding ? WindowFrameBoundType::kOffsetPreceding
                         : WindowFrameBoundType::kOffsetFollowing;
  for (const auto& child : node.children) {
    if (child->kind != "Location") {
      bound.offset = VisitExpression(*child);
      break;
    }
  }
  return bound;
}

NamedWindowParts ParseWindowSpecification(const GoogleSqlAstNode& spec) {
  NamedWindowParts parts;
  if (const GoogleSqlAstNode* partition = spec.Child("PartitionBy")) {
    for (const auto& child : partition->children) {
      if (child->kind == "PathExpression") {
        parts.partition_by.push_back(VisitExpression(*child));
      }
    }
  }
  if (const GoogleSqlAstNode* order = spec.Child("OrderBy")) {
    parts.order_by = ParseOrderingList(*order);
  }
  if (const GoogleSqlAstNode* frame = spec.Child("WindowFrame")) {
    const std::string unit = UpperCopy(frame->detail);
    parts.frame_unit = unit == "RANGE"    ? WindowFrameUnit::kRange
                       : unit == "GROUPS" ? WindowFrameUnit::kGroups
                                          : WindowFrameUnit::kRows;
    const auto bounds = frame->Children("WindowFrameExpr");
    // GoogleSQL allows only `CURRENT ROW` as a single-bound frame; offset
    // bounds require the full BETWEEN .. AND .. form (corpus row_number_3).
    if (bounds.size() >= 2) {
      parts.has_frame = true;
      parts.frame_start = ParseFrameBound(*bounds[0]);
      parts.frame_end = ParseFrameBound(*bounds[1]);
    } else if (bounds.size() == 1 &&
               UpperCopy(bounds[0]->detail).find("CURRENT ROW") !=
                   std::string::npos) {
      // `BETWEEN CURRENT ROW AND CURRENT ROW` shorthand.
      parts.has_frame = true;
      parts.frame_start = ParseFrameBound(*bounds[0]);
      parts.frame_end = parts.frame_start;
    } else if (bounds.size() == 1) {
      // Single-bound shorthand: `x PRECEDING` / `UNBOUNDED PRECEDING` mean
      // `BETWEEN x AND CURRENT ROW`.  Offset bounds additionally require an
      // ORDER BY clause in the window specification.
      const std::string bound_text = UpperCopy(bounds[0]->detail);
      if (bound_text.find("PRECEDING") != std::string::npos &&
          bound_text.find("UNBOUNDED") == std::string::npos &&
          parts.order_by.empty()) {
        throw std::runtime_error(
            "GoogleSQL AST: window frame requires BETWEEN x AND y");
      }
      parts.has_frame = true;
      parts.frame_start = ParseFrameBound(*bounds[0]);
      WindowFrameBound current;
      current.type = WindowFrameBoundType::kCurrentRow;
      parts.frame_end = current;
    } else {
      throw std::runtime_error(
          "GoogleSQL AST: window frame requires BETWEEN x AND y");
    }
    for (const auto& child : frame->children) {
      const std::string kind = UpperCopy(child->kind);
      const std::string detail = UpperCopy(child->detail);
      if (kind.find("EXCLUDE") == std::string::npos &&
          detail.find("EXCLUDE") == std::string::npos) {
        continue;
      }
      if (detail.find("CURRENT ROW") != std::string::npos ||
          kind.find("CURRENT") != std::string::npos) {
        parts.exclusion = WindowFrameExclusion::kCurrentRow;
      } else if (detail.find("GROUP") != std::string::npos ||
                 kind.find("GROUP") != std::string::npos) {
        parts.exclusion = WindowFrameExclusion::kGroup;
      } else if (detail.find("TIES") != std::string::npos ||
                 kind.find("TIES") != std::string::npos) {
        parts.exclusion = WindowFrameExclusion::kTies;
      }
    }
  }
  return parts;
}

Expression VisitAnalyticFunctionCall(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* call = node.Child("FunctionCall");
  if (call == nullptr || call->children.empty()) {
    throw std::runtime_error("GoogleSQL AST: malformed analytic function call");
  }
  auto window = std::make_shared<WindowFunctionCallExpression>();

  size_t arg_start = 0;
  if (call->children[0]->kind == "PathExpression") {
    window->function = UpperCopy(Path(*call->children[0]));
    arg_start = 1;
  } else {
    throw std::runtime_error("GoogleSQL AST: anonymous analytic function");
  }
  if (call->detail.find("distinct=true") != std::string::npos) {
    window->distinct = true;
  }

  std::vector<WindowOrderTerm> inner_order_by;
  for (size_t i = arg_start; i < call->children.size(); ++i) {
    const GoogleSqlAstNode& child = *call->children[i];
    if (child.kind == "Location") {
      continue;
    }
    if (child.kind == "WhereClause") {
      // AGG(x WHERE cond) OVER (...): row-level pre-filter.
      if (!child.children.empty()) {
        window->where_filter = VisitExpression(*child.children[0]);
      }
      continue;
    }
    if (child.kind == "OrderBy") {
      for (WindowOrderTerm& term : ParseOrderingList(child)) {
        inner_order_by.push_back(std::move(term));
      }
      continue;
    }
    if (child.kind == "LimitOffset") {
      if (const GoogleSqlAstNode* limit_node = child.Child("Limit")) {
        if (const GoogleSqlAstNode* value = limit_node->Child("IntLiteral")) {
          window->inner_limit =
              static_cast<size_t>(ParseUnsignedLiteral(*value));
        }
      }
      continue;
    }
    window->args.push_back(VisitExpression(child));
  }
  window->inner_order_by = std::move(inner_order_by);

  const GoogleSqlAstNode* spec = node.Child("WindowSpecification");
  if (spec != nullptr) {
    if (spec->Child("Identifier") != nullptr &&
        spec->Child("PartitionBy") == nullptr &&
        spec->Child("OrderBy") == nullptr &&
        spec->Child("WindowFrame") == nullptr) {
      // Bare reference to a WINDOW-clause definition.
      const std::string name = Identifier(*spec->Child("Identifier"));
      const auto found = t_named_windows.find(name);
      if (found == t_named_windows.end()) {
        throw std::runtime_error("GoogleSQL AST: unknown window " + name);
      }
      window->partition_by = found->second.partition_by;
      window->order_by = found->second.order_by;
      window->frame_unit = found->second.frame_unit;
      window->frame_start = found->second.frame_start;
      window->frame_end = found->second.frame_end;
      window->has_frame = found->second.has_frame;
      window->exclusion = found->second.exclusion;
    } else {
      NamedWindowParts parts = ParseWindowSpecification(*spec);
      window->partition_by = std::move(parts.partition_by);
      window->order_by = std::move(parts.order_by);
      window->frame_unit = parts.frame_unit;
      window->frame_start = parts.frame_start;
      window->frame_end = parts.frame_end;
      window->has_frame = parts.has_frame;
      window->exclusion = parts.exclusion;
    }
  }
  return window;
}

Expression VisitExpression(
    const GoogleSqlAstNode&
        node) {  // NOLINT(misc-no-recursion) // Recursive AST descent by
                 // design; stack overflow guarded via ExpressionDepthGuard.
  const ExpressionDepthGuard depth_guard;
  if (node.kind == "PathExpression") {
    std::string path_name = Path(node);
    if (HasSessionConstant(path_name)) {
      return ConstantValueExp(Value(GetSessionConstant(path_name)));
    }
    return ColumnValueExp(path_name);
  }
  if (node.kind == "Star") {
    return ColumnValueExp("*");
  }
  if (node.kind == "IntLiteral") {
    const std::string& text = node.detail;
    // UINT64-range literals cannot fit int64; keep them as their nearest
    // double so queries over uint64 columns still execute.
    if (text.size() > 18) {
      try {
        uint64_t wide = ParseUnsignedLiteral(node);
        if (wide > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          return ConstantValueExp(Value(static_cast<double>(wide)));
        }
      } catch (...) {
      }
    }
    return ConstantValueExp(Value(ParseIntLiteral(node)));
  }
  if (node.kind == "FloatLiteral") {
    return ConstantValueExp(Value(ParseFloatLiteral(node)));
  }
  if (node.kind == "StringLiteral") {
    return ConstantValueExp(Value(DecodeString(node)));
  }
  if (node.kind == "JSONLiteral" || node.kind == "NumericLiteral" ||
      node.kind == "BigNumericLiteral") {
    // JSON/NUMERIC/BIGNUMERIC literals carry their text in a nested
    // StringLiteral; tinylamb represents these types as their textual form.
    const GoogleSqlAstNode* literal = node.Child("StringLiteral");
    if (literal == nullptr) {
      for (const auto& child : node.children) {
        if (child->kind == "StringLiteral") {
          literal = child.get();
          break;
        }
      }
    }
    if (literal == nullptr) {
      throw std::runtime_error("GoogleSQL AST: malformed " + node.kind);
    }
    return ConstantValueExp(Value(DecodeString(*literal)));
  }
  if (node.kind == "BytesLiteral") {
    return ConstantValueExp(Value(DecodeBytes(node)));
  }
  if (node.kind == "DateOrTimeLiteral") {
    const GoogleSqlAstNode* literal = node.Child("StringLiteral");
    if (literal == nullptr) {
      throw std::runtime_error("GoogleSQL AST: invalid date/time literal");
    }
    const std::string text = DecodeString(*literal);
    if (node.detail == "TYPE_DATE") {
      return ConstantValueExp(Value::Date(text));
    }
    if (node.detail == "TYPE_DATETIME") {
      std::string dt_str = text;
      if (dt_str.find(":59:60") != std::string::npos) {
        int Y = 0, M = 0, D = 0, h = 0;
        if (sscanf(dt_str.c_str(), "%d-%d-%d %d", &Y, &M, &D, &h) >= 4) {
          h += 1;
          std::chrono::year_month_day ymd{
              std::chrono::year{Y},
              std::chrono::month{static_cast<unsigned>(M)},
              std::chrono::day{static_cast<unsigned>(D)}};
          int64_t days =
              std::chrono::sys_days{ymd}.time_since_epoch().count() + h / 24;
          h %= 24;
          std::chrono::sys_days new_sd{std::chrono::days{days}};
          std::chrono::year_month_day new_ymd{new_sd};
          char buf[64];
          snprintf(buf, sizeof(buf), "%04d-%02u-%02u %02d:00:00",
                   int(new_ymd.year()), unsigned(new_ymd.month()),
                   unsigned(new_ymd.day()), h);
          dt_str = buf;
        }
      }
      return ConstantValueExp(Value(std::move(dt_str)));
    }
    if (node.detail == "TYPE_TIME") {
      return ConstantValueExp(Value(std::string(text)));
    }
    // TYPE_TIMESTAMP
    std::string norm_ts = text;
    if (text.size() >= 10) {
      size_t tz_pos = std::string::npos;
      for (size_t i = 10; i < text.size(); ++i) {
        if (text[i] == '+' || text[i] == '-') {
          tz_pos = i;
          break;
        }
      }
      int total_offset_mins = 0;
      bool has_explicit_tz = false;
      if (text.find("UTC") != std::string::npos ||
          text.find("utc") != std::string::npos ||
          text.find('Z') != std::string::npos ||
          text.find('z') != std::string::npos) {
        total_offset_mins = 0;
        has_explicit_tz = true;
      }
      std::string base_time = text;
      if (tz_pos != std::string::npos) {
        has_explicit_tz = true;
        base_time = text.substr(0, tz_pos);
        char sign = text[tz_pos];
        std::string tz_part = text.substr(tz_pos + 1);
        int tz_hours = 0, tz_mins = 0;
        size_t colon = tz_part.find(':');
        if (colon != std::string::npos) {
          try {
            tz_hours = std::stoi(tz_part.substr(0, colon));
            tz_mins = std::stoi(tz_part.substr(colon + 1));
          } catch (...) {
          }
        } else {
          try {
            tz_hours = std::stoi(tz_part);
          } catch (...) {
          }
        }
        total_offset_mins = (tz_hours * 60 + tz_mins) * (sign == '-' ? -1 : 1);
      }
      bool is_leap_sec = false;
      if (base_time.find(":59:60") != std::string::npos) {
        is_leap_sec = true;
        size_t pos = base_time.find(":59:60");
        base_time.replace(pos, 6, ":59:00");
      }
      int Y = 0, M = 0, D = 0, h = 0, m = 0;
      double s = 0;
      int matched = sscanf(base_time.c_str(), "%d-%d-%d %d:%d:%lf", &Y, &M, &D,
                           &h, &m, &s);
      if (matched < 3) {
        matched = sscanf(base_time.c_str(), "%d-%d-%d", &Y, &M, &D);
      }
      if (matched >= 3) {
        if (is_leap_sec) {
          m += 1;
          s = 0.0;
        }
        if (!has_explicit_tz) {
          total_offset_mins =
              ParseTimeZoneOffset(GetDefaultTimeZone(), Y, M, D, h, m,
                                  static_cast<int>(s), -8 * 3600) /
              60;
        }
        struct tm t = {};
        t.tm_year = Y - 1900;
        t.tm_mon = M - 1;
        t.tm_mday = D;
        t.tm_hour = h;
        t.tm_min = m - total_offset_mins;
        t.tm_sec = static_cast<int>(s);
        t.tm_isdst = 0;
        time_t epoch = timegm(&t);
        struct tm utc = {};
        gmtime_r(&epoch, &utc);
        char buf[64];
        size_t dot_pos = text.find('.');
        if (dot_pos != std::string::npos) {
          size_t end_digit = dot_pos + 1;
          while (end_digit < text.size() && text[end_digit] >= '0' &&
                 text[end_digit] <= '9') {
            ++end_digit;
          }
          std::string frac_str = text.substr(dot_pos, end_digit - dot_pos);
          snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d%s+00",
                   utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday, utc.tm_hour,
                   utc.tm_min, utc.tm_sec, frac_str.c_str());
        } else {
          snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d+00",
                   utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday, utc.tm_hour,
                   utc.tm_min, utc.tm_sec);
        }
        norm_ts = buf;
      }
    }
    return ConstantValueExp(Value(std::move(norm_ts)));
  }

  if (node.kind == "NullLiteral") {
    return ConstantValueExp(Value());
  }
  if (node.kind == "BooleanLiteral") {
    return ConstantValueExp(Value(IsBooleanLiteralDetail(node.detail, true)));
  }
  if (node.kind == "ArrayConstructor") {
    std::string element_type;
    std::vector<Expression> elements;
    for (const auto& child : node.children) {
      if (IsAstTrivia(child->kind)) {
        continue;
      }
      if (IsArrayTypeNode(child->kind)) {
        for (const auto& nested : child->children) {
          const std::string parsed = SqlTypeFromAst(*nested);
          if (!parsed.empty()) {
            element_type = parsed;
            break;
          }
        }
        continue;
      }
      elements.push_back(VisitExpression(*child));
    }
    if (element_type.empty()) {
      for (const auto& child : node.children) {
        if (IsAstTrivia(child->kind) || IsArrayTypeNode(child->kind)) {
          continue;
        }
        element_type = InferArrayElementSqlType(*child);
        if (!element_type.empty()) {
          break;
        }
      }
    }
    if (element_type.empty()) {
      element_type = "INT64";
    }
    return ArrayExpressionExp(std::move(elements), std::move(element_type));
  }
  if (node.kind == "BinaryExpression") {
    if (node.children.size() != 2) {
      throw std::runtime_error("GoogleSQL AST: binary expression arity");
    }
    Expression left = VisitExpression(*node.children[0]);
    if (node.detail.find("DISTINCT FROM") != std::string::npos) {
      // `a IS [NOT] DISTINCT FROM b`: never UNKNOWN; NULLs compare equal.
      const bool negated =
          node.detail.starts_with("IS NOT") || node.detail == "NOT DISTINCT";
      Expression call = FunctionCallExp(
          "__is_distinct_from",
          {std::move(left), VisitExpression(*node.children[1])});
      return negated ? UnaryExpressionExp(std::move(call), UnaryOperation::kNot)
                     : std::move(call);
    }
    if (node.detail == "IS" || node.detail == "IS NOT") {
      const bool is_not = (node.detail == "IS NOT");
      const std::string rhs_kind = node.children[1]->kind;
      const std::string rhs_detail = node.children[1]->detail;
      if (rhs_kind == "NullLiteral") {
        return UnaryExpressionExp(
            std::move(left),
            is_not ? UnaryOperation::kIsNotNull : UnaryOperation::kIsNull);
      }
      if (rhs_kind == "BooleanLiteral") {
        const bool val = IsBooleanLiteralDetail(rhs_detail, true);
        if (val) {
          return UnaryExpressionExp(
              std::move(left),
              is_not ? UnaryOperation::kIsNotTrue : UnaryOperation::kIsTrue);
        }
        return UnaryExpressionExp(
            std::move(left),
            is_not ? UnaryOperation::kIsNotFalse : UnaryOperation::kIsFalse);
      }
      if (rhs_kind == "PathExpression" || rhs_kind == "Identifier") {
        const std::string ident = Lower(Path(*node.children[1]));
        if (ident == "true") {
          return UnaryExpressionExp(
              std::move(left),
              is_not ? UnaryOperation::kIsNotTrue : UnaryOperation::kIsTrue);
        }
        if (ident == "false") {
          return UnaryExpressionExp(
              std::move(left),
              is_not ? UnaryOperation::kIsNotFalse : UnaryOperation::kIsFalse);
        }
        if (ident == "null" || ident == "unknown") {
          return UnaryExpressionExp(
              std::move(left),
              is_not ? UnaryOperation::kIsNotNull : UnaryOperation::kIsNull);
        }
      }
    }
    if (node.detail == "+" || node.detail == "-" || node.detail == "*" ||
        node.detail == "/" || node.detail == "%") {
      if (node.children[0]->kind == "NullLiteral" ||
          node.children[1]->kind == "NullLiteral") {
        throw std::runtime_error("GoogleSQL AST: Operands of " + node.detail +
                                 " cannot be literal NULL");
      }
    }
    Expression right = VisitExpression(*node.children[1]);
    return MakeComparison(std::move(left), node.detail, std::move(right));
  }

  if (node.kind == "AndExpr") {
    return FoldBoolean(node, BinaryOperation::kAnd);
  }
  if (node.kind == "OrExpr") {
    return FoldBoolean(node, BinaryOperation::kOr);
  }
  if (node.kind == "UnaryExpression") {
    if (node.children.size() != 1) {
      throw std::runtime_error("GoogleSQL AST: unary expression arity");
    }
    if (node.detail == "-") {
      if (node.children[0]->kind == "IntLiteral" &&
          node.children[0]->detail == "9223372036854775808") {
        return ConstantValueExp(Value(std::numeric_limits<int64_t>::min()));
      }
    }
    if (node.detail == "NOT") {
      if (node.children[0]->kind == "NullLiteral") {
        throw std::runtime_error(
            "GoogleSQL AST: Operands of NOT cannot be literal NULL");
      }
      if (node.children[0]->kind == "UnaryExpression" &&
          node.children[0]->detail == "NOT") {
        const auto* grand = node.children[0]->children.empty()
                                ? nullptr
                                : node.children[0]->children[0].get();
        if (grand && grand->kind == "NullLiteral") {
          throw std::runtime_error(
              "GoogleSQL AST: Operands of NOT cannot be literal NULL");
        }
      }
    }
    return UnaryExpressionExp(
        VisitExpression(*node.children[0]),
        node.detail == "NOT" ? UnaryOperation::kNot : UnaryOperation::kMinus);
  }

  if (node.kind == "ConcatExpr") {
    std::vector<Expression> args;
    args.reserve(node.children.size());
    for (const auto& child : node.children) {
      if (child->kind == "Location") {
        continue;
      }
      Expression arg = VisitExpression(*child);
      arg = WrapIfBoolean(std::move(arg), *child);
      args.push_back(std::move(arg));
    }
    return FunctionCallExp("concat", std::move(args));
  }

  if (node.kind == "FunctionCall") {
    return VisitFunction(node);
  }

  if (node.kind == "AnalyticFunctionCall") {
    return VisitAnalyticFunctionCall(node);
  }

  if (node.kind == "ExpressionWithAlias") {
    // Alias handled by enclosing contexts (select list); unwrap here.
    for (const auto& child : node.children) {
      if (child->kind != "Location" && child->kind != "Alias") {
        return VisitExpression(*child);
      }
    }
    throw std::runtime_error("GoogleSQL AST: empty aliased expression");
  }

  if (node.kind == "ArrayElement") {
    // a[OFFSET(n)] / a[ORDINAL(n)] / a[n] (OFFSET semantics by default).
    Expression base;
    const GoogleSqlAstNode* index_node = nullptr;
    std::string accessor;
    for (const auto& child : node.children) {
      if (child->kind == "Location") {
        continue;
      }
      if (child->kind == "FunctionCall") {
        std::string callee;
        if (!child->children.empty() &&
            child->children.front()->kind == "PathExpression") {
          callee = UpperCopy(Path(*child->children.front()));
        }
        const bool looks_like_accessor =
            callee.find("OFFSET") != std::string::npos ||
            callee.find("ORDINAL") != std::string::npos || callee == "ROW" ||
            callee.empty();
        if (!looks_like_accessor) {
          if (!base) {
            base = VisitExpression(*child);
          }
          continue;
        }
        if (!child->children.empty() &&
            child->children.front()->kind == "PathExpression") {
          accessor = callee;
          const GoogleSqlAstNode* arg = nullptr;
          for (size_t i = 1; i < child->children.size(); ++i) {
            if (child->children[i]->kind != "Location") {
              arg = child->children[i].get();
              break;
            }
          }
          if (arg != nullptr && index_node == nullptr) {
            index_node = arg;
          }
        }
        continue;
      }
      if (!base && child->kind != "Location") {
        base = VisitExpression(*child);
      } else if (base && !index_node && child->kind != "Location") {
        // Bare `a[0]` form: a plain literal index follows the base.
        index_node = child.get();
      }
    }
    if (!base || index_node == nullptr) {
      throw std::runtime_error("GoogleSQL AST: malformed array element");
    }
    std::string fn = "array_element_offset";
    const bool safe_access = accessor.find("SAFE") != std::string::npos;
    if (accessor.find("ORDINAL") != std::string::npos) {
      fn = safe_access ? "safe_array_element_ordinal" : "array_element_ordinal";
    } else if (safe_access) {
      fn = "safe_array_element_offset";
    }
    return FunctionCallExp(fn, {std::move(base), VisitExpression(*index_node)});
  }

  if (node.kind == "DotStar") {
    // `relation.*`: qualified star expanded during projection.
    const GoogleSqlAstNode* path = node.Child("PathExpression");
    if (path == nullptr) {
      throw std::runtime_error("GoogleSQL AST: malformed DotStar");
    }
    ColumnName name(Path(*path), "*");
    return ColumnValueExp(name);
  }

  if (node.kind == "CaseValueExpression") {
    if (node.children.empty()) {
      throw std::runtime_error("GoogleSQL AST: empty CASE");
    }
    Expression value_expr = VisitExpression(*node.children[0]);
    std::vector<std::pair<Expression, Expression>> clauses;
    size_t pair_end = node.children.size();
    Expression otherwise = ConstantValueExp(Value());
    if ((pair_end - 1) % 2 == 1) {
      otherwise = VisitExpression(*node.children.back());
      --pair_end;
    }
    for (size_t i = 1; i < pair_end; i += 2) {
      Expression when_val = VisitExpression(*node.children[i]);
      Expression cond = MakeComparison(value_expr, "=", std::move(when_val));
      clauses.emplace_back(std::move(cond),
                           VisitExpression(*node.children[i + 1]));
    }
    return CaseExpressionExp(std::move(clauses), std::move(otherwise));
  }
  if (node.kind == "CaseNoValueExpression") {
    if (node.children.empty()) {
      throw std::runtime_error("GoogleSQL AST: empty CASE");
    }
    std::vector<std::pair<Expression, Expression>> clauses;
    size_t pair_end = node.children.size();
    Expression otherwise = ConstantValueExp(Value());
    if (pair_end % 2 == 1) {
      otherwise = VisitExpression(*node.children.back());
      --pair_end;
    }
    for (size_t i = 0; i < pair_end; i += 2) {
      clauses.emplace_back(VisitExpression(*node.children[i]),
                           VisitExpression(*node.children[i + 1]));
    }
    return CaseExpressionExp(std::move(clauses), std::move(otherwise));
  }
  if (node.kind == "BetweenExpression") {
    std::vector<const GoogleSqlAstNode*> operands;
    for (const auto& child : node.children) {
      if (child->kind != "Location") {
        operands.push_back(child.get());
      }
    }
    if (operands.size() != 3) {
      throw std::runtime_error("GoogleSQL AST: BETWEEN arity");
    }
    Expression lower = MakeComparison(VisitExpression(*operands[0]),
                                      ">=", VisitExpression(*operands[1]));
    Expression upper = MakeComparison(VisitExpression(*operands[0]),
                                      "<=", VisitExpression(*operands[2]));
    Expression result = BinaryExpressionExp(
        std::move(lower), BinaryOperation::kAnd, std::move(upper));
    if (node.detail == "NOT BETWEEN") {
      result = UnaryExpressionExp(std::move(result), UnaryOperation::kNot);
    }
    return result;
  }
  if (node.kind == "InExpression") {
    if (node.children.empty()) {
      throw std::runtime_error("GoogleSQL AST: empty IN");
    }
    Expression test = VisitExpression(*node.children.front());
    const bool negated = node.detail == "NOT IN";
    if (const GoogleSqlAstNode* list = node.Child("InList")) {
      std::vector<Expression> values;
      values.reserve(list->children.size());
      for (const auto& child : list->children) {
        values.push_back(VisitExpression(*child));
      }
      Expression result = InExpressionExp(std::move(test), std::move(values));
      return negated
                 ? UnaryExpressionExp(std::move(result), UnaryOperation::kNot)
                 : result;
    }
    // `x IN UNNEST(arr)` / `x IN <array expr>`.
    if (const GoogleSqlAstNode* unnest = node.Child("UnnestExpression")) {
      Expression arr;
      for (const auto& child : unnest->children) {
        if (child->kind == "ExpressionWithOptAlias") {
          for (const auto& inner : child->children) {
            if (inner->kind != "Location" && inner->kind != "Alias") {
              arr = VisitExpression(*inner);
              break;
            }
          }
          break;
        }
      }
      if (!arr) {
        throw std::runtime_error("GoogleSQL AST: empty IN UNNEST");
      }
      Expression result = FunctionCallExp(
          "__quantified__", {std::move(test), std::move(arr),
                             ConstantValueExp(Value(std::string("="))),
                             ConstantValueExp(Value(std::string("ANY")))});
      return negated
                 ? UnaryExpressionExp(std::move(result), UnaryOperation::kNot)
                 : result;
    }
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query == nullptr) {
      throw std::runtime_error("GoogleSQL AST: IN without values");
    }
    return QueryExpressionExp(VisitQuery(*query), std::move(test), false,
                              negated);
  }
  if (node.kind == "ExpressionSubquery") {
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query == nullptr) {
      throw std::runtime_error("GoogleSQL AST: subquery without query");
    }
    if (node.detail == "modifier=ARRAY") {
      return ArraySubqueryExpressionExp(VisitQuery(*query));
    }
    return QueryExpressionExp(VisitQuery(*query), nullptr,
                              node.detail == "modifier=EXISTS", false);
  }
  if (node.kind == "ExtractExpression") {
    if (node.children.size() < 2) {
      throw std::runtime_error("GoogleSQL AST: EXTRACT arity");
    }
    const std::string part = Lower(Path(*node.children[0]));
    std::vector<Expression> args;
    args.push_back(VisitExpression(*node.children[1]));
    if (node.children.size() >= 3) {
      args.push_back(VisitExpression(*node.children[2]));
    }
    return FunctionCallExp("extract_" + part, std::move(args));
  }
  if (node.kind == "IntervalExpr") {
    std::string unit = "second";
    const GoogleSqlAstNode* value_node = nullptr;
    std::vector<std::string> unit_parts;
    for (const auto& child : node.children) {
      if (child->kind == "Identifier" || child->kind == "DateOrTimeUnit") {
        unit_parts.push_back(Lower(Identifier(*child)));
      } else if (child->kind == "DateOrTimeUnitRange") {
        for (const auto& uc : child->children) {
          if (uc->kind == "Identifier" || uc->kind == "DateOrTimeUnit") {
            unit_parts.push_back(Lower(Identifier(*uc)));
          }
        }
      } else if (child->kind != "Location") {
        value_node = child.get();
      }
    }
    if (!unit_parts.empty()) {
      if (unit_parts.size() == 1) {
        unit = unit_parts[0];
      } else if (unit_parts.size() >= 2) {
        unit = unit_parts[0] + " to " + unit_parts[1];
      }
    }
    if (value_node != nullptr) {
      if (value_node->kind == "StringLiteral") {
        std::string str_val = DecodeString(*value_node);
        int64_t amount = 0;
        try {
          amount = std::stoll(str_val);
        } catch (...) {
        }
        return IntervalExpressionExp(amount, std::move(unit),
                                     std::move(str_val));
      }
      Expression expr = VisitExpression(*value_node);
      if (expr->Type() == TypeTag::kConstantValue) {
        const Value& v = expr->AsConstantValue().GetValue();
        if (v.type == ValueType::kInt64) {
          return IntervalExpressionExp(v.value.int_value, std::move(unit));
        }
        if (v.type == ValueType::kVarChar) {
          std::string str_val = std::string(v.value.varchar_value);
          int64_t amount = 0;
          try {
            amount = std::stoll(str_val);
          } catch (...) {
          }
          return IntervalExpressionExp(amount, std::move(unit),
                                       std::move(str_val));
        }
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        std::string col_name = expr->AsColumnValue().GetColumnName().name;
        if (HasSessionConstant(col_name)) {
          std::string str_val = GetSessionConstant(col_name);
          return IntervalExpressionExp(0, std::move(unit), std::move(str_val));
        }
        return FunctionCallExp(
            "make_interval",
            {expr, ConstantValueExp(Value(std::string(unit)))});
      }
      Row dummy_row;
      Schema dummy_schema;
      try {
        Value v = expr->Evaluate(dummy_row, dummy_schema);
        if (v.type == ValueType::kInt64) {
          return IntervalExpressionExp(v.value.int_value, std::move(unit));
        }
        if (v.type == ValueType::kVarChar) {
          std::string str_val = std::string(v.value.varchar_value);
          int64_t amount = 0;
          try {
            amount = std::stoll(str_val);
          } catch (...) {
          }
          return IntervalExpressionExp(amount, std::move(unit),
                                       std::move(str_val));
        }
      } catch (...) {
      }
      return FunctionCallExp(
          "make_interval", {expr, ConstantValueExp(Value(std::string(unit)))});
    }
    if (!node.children.empty() && node.children[0]->kind == "StringLiteral") {
      std::string res = DecodeString(*node.children[0]);
      return ConstantValueExp(Value(std::move(res)));
    }
    throw std::runtime_error("GoogleSQL AST: unsupported interval");
  }
  if (node.kind == "ParameterExpr") {
    const GoogleSqlAstNode* ident = node.Child("Identifier");
    if (ident != nullptr) {
      return ColumnValueExp(ident->detail);
    }
    throw std::runtime_error("GoogleSQL AST: invalid parameter expression");
  }
  if (node.kind == "RangeLiteral") {
    if (const GoogleSqlAstNode* str = node.Child("StringLiteral")) {
      return ConstantValueExp(Value(DecodeString(*str)));
    }
    for (const auto& child : node.children) {
      if (child->kind == "StringLiteral") {
        return ConstantValueExp(Value(DecodeString(*child)));
      }
    }
    return ConstantValueExp(Value(std::string(node.detail)));
  }
  if (node.kind == "DateOrTimeUnit") {
    if (const GoogleSqlAstNode* id = node.Child("Identifier")) {
      return ConstantValueExp(Value(Identifier(*id)));
    }
    std::string unit = node.detail;
    if (unit.starts_with("unit=")) {
      unit = unit.substr(5);
    }
    return ConstantValueExp(Value(std::move(unit)));
  }
  if (node.kind == "CastExpression") {
    if (node.children.size() < 2) {
      throw std::runtime_error("GoogleSQL AST: CAST without operand or type");
    }
    Expression child = VisitExpression(*node.children[0]);
    Expression child_expr = child;
    std::string type_name = SqlTypeFromAst(*node.children[1]);
    if (type_name.empty()) {
      if (const auto* path = node.children[1]->Child("PathExpression")) {
        type_name = Path(*path);
      } else if (node.children[1]->kind == "SimpleType") {
        for (const auto& c : node.children[1]->children) {
          type_name = SqlTypeFromAst(*c);
          if (!type_name.empty()) {
            break;
          }
        }
      }
    }
    if (type_name.empty()) {
      type_name = "STRING";
    }
    const std::string upper_type = UpperCopy(type_name);
    if (upper_type.ends_with("TESTENUM") || upper_type.ends_with("ENUM")) {
      if (child->Type() == TypeTag::kConstantValue) {
        const Value& v = child->AsConstantValue().GetValue();
        if (v.type == ValueType::kInt64) {
          return ConstantValueExp(
              Value("TESTENUM" + std::to_string(v.value.int_value)));
        }
      }
    }

    const bool safe =
        node.detail.find("return_null_on_error=true") != std::string::npos;
    // CAST(... AS type FORMAT '...' AT TIME ZONE '...') carries its format
    // and zone in a FormatClause sibling node.
    for (const auto& fmt : node.children) {
      if (fmt->kind != "FormatClause" || fmt->children.size() < 2) {
        continue;
      }
      const GoogleSqlAstNode* fmt_lit =
          fmt->children[0]->Child("StringLiteral");
      if (fmt_lit == nullptr) {
        fmt_lit = fmt->children[0].get();
      }
      std::string format_text = DecodeString(*fmt_lit);
      const GoogleSqlAstNode* tz_node =
          fmt->children[1]->Child("StringLiteral");
      if (tz_node == nullptr) {
        tz_node = fmt->children[1].get();
      }
      std::string tz_text = DecodeString(*tz_node);
      return FunctionCallExp(
          "__cast_format",
          {std::move(child), ConstantValueExp(Value(std::move(type_name))),
           ConstantValueExp(Value(std::move(format_text))),
           ConstantValueExp(Value(std::move(tz_text)))});
    }
    return CastExpressionExp(std::move(child), std::move(type_name), safe);
  }

  if (node.kind == "NewConstructor") {
    std::string text_format;
    for (const auto& child : node.children) {
      if (child->kind == "NewConstructorArg" && child->children.size() >= 2) {
        Expression val_expr = VisitExpression(*child->children[0]);
        std::string field_name = Identifier(*child->children[1]);
        std::string val_str;
        if (val_expr->Type() == TypeTag::kConstantValue) {
          val_str = val_expr->AsConstantValue().GetValue().AsString();
        }
        if (!text_format.empty()) {
          text_format += " ";
        }
        text_format += field_name + ": " + val_str;
      }
    }
    return ConstantValueExp(Value(std::move(text_format)));
  }

  if (node.kind == "StructConstructorWithKeyword" ||
      node.kind == "StructConstructorWithParens") {
    std::vector<std::string> field_names;
    if (const GoogleSqlAstNode* st = node.Child("StructType")) {
      for (const auto& child : st->children) {
        if (child->kind == "StructField") {
          if (const GoogleSqlAstNode* id = child->Child("Identifier")) {
            field_names.push_back(Identifier(*id));
          } else {
            field_names.push_back("");
          }
        }
      }
    }
    std::string json_out = "{";
    bool first = true;
    size_t arg_idx = 0;
    for (const auto& child : node.children) {
      if (child->kind == "Location" || child->kind == "StructType") {
        continue;
      }
      const GoogleSqlAstNode* arg_node = child.get();
      std::string fname =
          arg_idx < field_names.size() ? field_names[arg_idx] : "";
      if (child->kind == "StructConstructorArg") {
        if (fname.empty()) {
          // Field alias: STRUCT(1 AS emp_id) nests the Identifier under an
          // Alias node.
          const GoogleSqlAstNode* id = child->Child("Identifier");
          if (id == nullptr) {
            if (const GoogleSqlAstNode* alias = child->Child("Alias")) {
              id = alias->Child("Identifier");
            }
          }
          if (id != nullptr) {
            fname = Identifier(*id);
          }
        }
        for (const auto& arg_child : child->children) {
          if (arg_child->kind != "Location" &&
              arg_child->kind != "Identifier" && arg_child->kind != "Alias") {
            arg_node = arg_child.get();
            break;
          }
        }
      }
      if (fname.empty()) {
        fname = "f" + std::to_string(arg_idx + 1);
      }

      std::string val_str = "null";
      if (arg_node->kind == "BooleanLiteral") {
        val_str =
            IsBooleanLiteralDetail(arg_node->detail, true) ? "true" : "false";
      } else if (arg_node->kind == "NullLiteral") {
        val_str = "null";
      } else {
        Expression val_expr = VisitExpression(*arg_node);
        if (val_expr) {
          try {
            Value v = val_expr->Evaluate(Row(), Schema());
            if (!v.IsNull()) {
              if (v.type == ValueType::kVarChar) {
                std::string text(v.value.varchar_value);
                // Map/array payloads stay raw JSON; everything else is a
                // quoted string.
                const bool json_shaped =
                    (!text.empty() &&
                     ((text.front() == '{' && text.back() == '}') ||
                      (text.front() == '[' && text.back() == ']')));
                val_str = json_shaped ? text : "\"" + std::move(text) + "\"";
              } else if (v.type == ValueType::kInt64) {
                val_str = std::to_string(v.value.int_value);
              } else if (v.type == ValueType::kDouble) {
                std::ostringstream out;
                out << std::setprecision(17) << v.value.double_value;
                val_str = out.str();
              } else if (v.IsArray()) {
                val_str = ValueToJsonArray(v);
              } else {
                val_str = v.AsString();
              }
            }
          } catch (...) {
          }
        }
      }
      if (!first) {
        json_out += ",";
      }
      first = false;
      std::string escaped_fname;
      for (char c : fname) {
        if (c == '"') {
          escaped_fname += "\\\"";
        } else if (c == '\\') {
          escaped_fname += "\\\\";
        } else if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
          escaped_fname += buf;
        } else {
          escaped_fname.push_back(c);
        }
      }
      json_out += "\"" + escaped_fname + "\":" + val_str;
      ++arg_idx;
    }
    json_out += "}";
    return ConstantValueExp(Value(std::move(json_out)));
  }

  if (node.kind == "DotIdentifier") {
    if (node.children.size() >= 2 && node.children[1]->kind == "Identifier") {
      Expression base_expr = VisitExpression(*node.children[0]);
      std::string field_name = Identifier(*node.children[1]);
      return FunctionCallExp("get_field",
                             {std::move(base_expr),
                              ConstantValueExp(Value(std::move(field_name)))});
    }
    throw std::runtime_error("GoogleSQL AST: invalid DotIdentifier");
  }

  if (node.kind == "UnnestExpression") {
    // In scalar contexts (e.g. quantified comparisons) UNNEST simply yields
    // its underlying array expression.
    const GoogleSqlAstNode* inner = node.Child("ExpressionWithOptAlias");
    if (inner == nullptr || inner->children.empty()) {
      throw std::runtime_error("GoogleSQL AST: malformed UNNEST");
    }
    for (const auto& child : inner->children) {
      if (child->kind != "Location" && child->kind != "Identifier" &&
          child->kind != "Alias") {
        return VisitExpression(*child);
      }
    }
    throw std::runtime_error("GoogleSQL AST: empty UNNEST");
  }

  if (node.kind == "QuantifiedComparisonExpression") {
    // `lhs <op> ANY/ALL/SOME (list | query)`.  List forms desugar into OR/AND
    // comparison chains; the two subquery shapes with an equality mapping
    // (op = ANY -> IN, op <> ALL -> NOT IN) reuse the IN-subquery machinery.
    Expression lhs;
    std::string quantifier;
    const GoogleSqlAstNode* list_node = nullptr;
    const GoogleSqlAstNode* collection = nullptr;
    for (const auto& child : node.children) {
      if (child->kind == "Location") {
        continue;
      }
      if (child->kind == "AnySomeAllOp") {
        quantifier = UpperCopy(child->detail);
        continue;
      }
      if (child->kind == "InList") {
        list_node = child.get();
        continue;
      }
      if (!lhs) {
        lhs = VisitExpression(*child);
      } else if (!collection) {
        // UNNEST(array) / bare array expression collections.
        collection = child.get();
      }
    }
    if (!lhs || quantifier.empty()) {
      throw std::runtime_error(
          "GoogleSQL AST: malformed quantified comparison");
    }
    const BinaryOperation op = BinaryOp(node.detail);
    const bool is_any = quantifier == "ANY" || quantifier == "SOME";
    const GoogleSqlAstNode* query =
        list_node != nullptr ? list_node->Child("Query") : nullptr;
    if (query == nullptr) {
      for (const auto& child : node.children) {
        if (child->kind == "Query") {
          query = child.get();
          break;
        }
      }
    }
    if (query != nullptr) {
      auto inner = VisitQuery(*query);
      return QuantifiedQueryExpressionExp(std::move(inner), std::move(lhs),
                                          std::string(node.detail), is_any);
    }
    if (collection != nullptr) {
      // Generic array-collection form: evaluated by the runtime helper
      // __quantified__(lhs, array, op, mode).
      Expression arr = VisitExpression(*collection);
      std::string node_detail = node.detail;
      std::string quantifier_copy = quantifier;
      return FunctionCallExp(
          "__quantified__",
          {std::move(lhs), std::move(arr),
           ConstantValueExp(Value(std::move(node_detail))),
           ConstantValueExp(Value(std::move(quantifier_copy)))});
    }
    std::vector<Expression> items;
    for (const auto& child : list_node->children) {
      if (child->kind != "Location") {
        items.push_back(VisitExpression(*child));
      }
    }
    Expression chain;
    for (Expression& item : items) {
      Expression term = MakeComparison(lhs, node.detail, item);
      if (chain == nullptr) {
        chain = std::move(term);
      } else if (is_any) {
        chain = BinaryExpressionExp(std::move(chain), BinaryOperation::kOr,
                                    std::move(term));
      } else {
        chain = BinaryExpressionExp(std::move(chain), BinaryOperation::kAnd,
                                    std::move(term));
      }
    }
    return chain;
  }

  if (node.kind == "LikeExpression") {
    // `lhs [NOT] LIKE rhs`, plus the LIKE ANY/SOME/ALL (list) extension.
    Expression lhs;
    const GoogleSqlAstNode* any_op = nullptr;
    const GoogleSqlAstNode* list_node = nullptr;
    const GoogleSqlAstNode* unnest_node = nullptr;
    std::vector<Expression> operands;
    for (const auto& child : node.children) {
      if (child->kind == "Location") {
        continue;
      }
      if (child->kind == "AnySomeAllOp") {
        any_op = child.get();
        continue;
      }
      if (child->kind == "InList") {
        list_node = child.get();
        continue;
      }
      if (!lhs) {
        lhs = VisitExpression(*child);
      } else if (child->kind == "UnnestExpression" && any_op != nullptr) {
        unnest_node = child.get();
      } else {
        operands.push_back(VisitExpression(*child));
      }
    }
    const bool negated =
        UpperCopy(node.detail).find("NOT") != std::string::npos;
    // `lhs LIKE ALL UNNEST(arr)`: evaluated per element by the runtime
    // helper with three-valued SQL semantics.
    if (any_op != nullptr && unnest_node != nullptr) {
      Expression arr;
      for (const auto& child : unnest_node->children) {
        if (child->kind == "ExpressionWithOptAlias") {
          for (const auto& inner : child->children) {
            if (inner->kind != "Location" && inner->kind != "Alias") {
              arr = VisitExpression(*inner);
              break;
            }
          }
          break;
        }
      }
      if (!arr) {
        throw std::runtime_error("GoogleSQL AST: empty LIKE UNNEST");
      }
      const bool is_any = UpperCopy(any_op->detail) == "ANY" ||
                          UpperCopy(any_op->detail) == "SOME";
      Expression call = FunctionCallExp(
          "__quantified__",
          {std::move(lhs), std::move(arr),
           ConstantValueExp(Value(std::string("LIKE"))),
           ConstantValueExp(Value(std::string(is_any ? "ANY" : "ALL")))});
      return negated ? UnaryExpressionExp(std::move(call), UnaryOperation::kNot)
                     : std::move(call);
    }
    if (any_op == nullptr || list_node == nullptr) {
      if (operands.empty()) {
        throw std::runtime_error("GoogleSQL AST: malformed LIKE");
      }
      Expression call =
          BinaryExpressionExp(lhs, BinaryOperation::kLike, operands[0]);
      return negated ? UnaryExpressionExp(std::move(call), UnaryOperation::kNot)
                     : Expression(call);
    }
    // LIKE ANY/ALL over a pattern list.
    const bool is_any = UpperCopy(any_op->detail) == "ANY" ||
                        UpperCopy(any_op->detail) == "SOME";
    std::vector<Expression> patterns;
    for (const auto& child : list_node->children) {
      if (child->kind != "Location") {
        patterns.push_back(VisitExpression(*child));
      }
    }
    Expression chain;
    for (Expression& pattern : patterns) {
      Expression term =
          BinaryExpressionExp(lhs, BinaryOperation::kLike, pattern);
      if (negated) {
        term = UnaryExpressionExp(std::move(term), UnaryOperation::kNot);
      }
      if (chain == nullptr) {
        chain = std::move(term);
      } else if (is_any) {
        chain = BinaryExpressionExp(std::move(chain), BinaryOperation::kOr,
                                    std::move(term));
      } else {
        chain = BinaryExpressionExp(std::move(chain), BinaryOperation::kAnd,
                                    std::move(term));
      }
    }
    return chain;
  }

  if (node.kind == "Lambda") {
    // children = [param..., body]: each param is a PathExpression (single
    // identifier) or a parenthesized StructConstructorWithParens of them; the
    // last non-location child is the body.
    std::vector<std::string> parameters;
    const GoogleSqlAstNode* body_node = nullptr;
    for (auto it = node.children.rbegin(); it != node.children.rend(); ++it) {
      if ((*it)->kind == "Location") {
        continue;
      }
      body_node = it->get();
      break;
    }
    if (body_node == nullptr) {
      throw std::runtime_error("GoogleSQL AST: lambda without body");
    }
    bool collecting = true;
    for (const auto& child : node.children) {
      if (child->kind == "Location") {
        continue;
      }
      if (child.get() == body_node) {
        break;
      }
      if (!collecting) {
        continue;
      }
      if (child->kind == "Identifier") {
        parameters.push_back(Lower(Identifier(*child)));
      } else if (child->kind == "PathExpression") {
        parameters.push_back(Lower(Path(*child)));
      } else if (child->kind == "StructConstructorWithParens" ||
                 child->kind == "StructConstructorWithKeyword" ||
                 child->kind == "ExpressionList") {
        for (const auto& param : child->children) {
          if (param->kind == "Location") {
            continue;
          }
          if (param->kind == "Identifier") {
            parameters.push_back(Lower(Identifier(*param)));
          } else if (param->kind == "PathExpression") {
            parameters.push_back(Lower(Path(*param)));
          } else if (!param->children.empty() &&
                     param->children.front()->kind == "PathExpression") {
            parameters.push_back(Lower(Identifier(*param->children.front())));
          }
        }
      } else {
        throw std::runtime_error(
            "GoogleSQL AST: unsupported lambda parameter " + child->kind);
      }
      collecting = true;
    }
    Expression body = VisitExpression(*body_node);
    return LambdaExpressionExp(std::move(parameters), std::move(body));
  }

  if (node.kind == "DefaultLiteral") {
    // DEFAULT in DML VALUES maps to the column default, which tinylamb
    // represents as NULL.
    return ConstantValueExp(Value());
  }
  if (node.kind == "NamedArgument") {
    // `name => value`: parameter names are not modeled; evaluate positionally.
    for (const auto& child : node.children) {
      if (child->kind != "Location" && child->kind != "Identifier") {
        return VisitExpression(*child);
      }
    }
    throw std::runtime_error("GoogleSQL AST: empty named argument");
  }

  throw std::runtime_error("GoogleSQL AST: unsupported expression " +
                           node.kind);
}

SelectSource VisitTableSource(
    const GoogleSqlAstNode& node,
    JoinType join_type,  // NOLINT(misc-no-recursion) // Recursive AST descent
                         // for nested joins/subqueries by design (see
                         // VisitQuery depth note).
    Expression join_condition) {
  SelectSource source;
  source.join_type = join_type;
  source.join_condition = std::move(join_condition);
  source.alias = Alias(node);
  if (node.kind == "TablePathExpression") {
    if (const GoogleSqlAstNode* unnest = node.Child("UnnestExpression")) {
      for (const auto& child : unnest->children) {
        if (child->kind == "ExpressionWithOptAlias") {
          for (const auto& expr_child : child->children) {
            if (expr_child->kind != "Location" && expr_child->kind != "Alias") {
              source.unnest = VisitExpression(*expr_child);
              break;
            }
          }
          if (source.alias.empty()) {
            source.alias = Alias(*child);
          }
          break;
        }
      }
      if (const GoogleSqlAstNode* with_offset = node.Child("WithOffset")) {
        if (const GoogleSqlAstNode* alias = with_offset->Child("Alias")) {
          source.offset_alias = Alias(*with_offset);
        } else {
          source.offset_alias = "offset";
        }
      } else if (const GoogleSqlAstNode* with_offset2 =
                     node.Child("WithOffsetClause")) {
        if (const GoogleSqlAstNode* alias = with_offset2->Child("Alias")) {
          source.offset_alias = Alias(*with_offset2);
        } else {
          source.offset_alias = "offset";
        }
      } else {
        for (const auto& child : node.children) {
          if (child->kind.find("Offset") != std::string::npos) {
            if (const GoogleSqlAstNode* alias = child->Child("Alias")) {
              source.offset_alias = Alias(*child);
            } else {
              source.offset_alias = "offset";
            }
            break;
          }
        }
      }
      if (source.alias.empty()) {
        source.alias = "unnest";
      }
      return source;
    }
    const GoogleSqlAstNode* path = node.Child("PathExpression");
    if (path == nullptr) {
      throw std::runtime_error("GoogleSQL AST: table without path");
    }
    source.table = Path(*path);
    if (source.alias.empty()) {
      source.alias = source.table;
    }
  } else if (node.kind == "TableSubquery") {
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query == nullptr) {
      throw std::runtime_error("GoogleSQL AST: table subquery missing query");
    }
    source.query = VisitQuery(*query);
  } else {
    throw std::runtime_error("GoogleSQL AST: unsupported table source " +
                             node.kind);
  }
  return source;
}

void AppendSources(const GoogleSqlAstNode& node,
                   JoinType incoming,  // NOLINT(misc-no-recursion) // Recursive
                                       // AST descent for nested joins by design
                                       // (see VisitQuery depth note).
                   Expression condition, std::vector<SelectSource>* sources) {
  if (node.kind != "Join") {
    sources->push_back(VisitTableSource(node, incoming, std::move(condition)));
    return;
  }
  std::vector<const GoogleSqlAstNode*> operands;
  const GoogleSqlAstNode* on = nullptr;
  const GoogleSqlAstNode* using_clause = nullptr;
  for (const auto& child : node.children) {
    if (child->kind == "TablePathExpression" ||
        child->kind == "TableSubquery" || child->kind == "Join") {
      operands.push_back(child.get());
    } else if (child->kind == "OnClause") {
      on = child.get();
    } else if (using_clause == nullptr && child->kind.starts_with("Using")) {
      using_clause = child.get();
    }
  }
  if (operands.size() != 2) {
    throw std::runtime_error("GoogleSQL AST: join arity");
  }
  AppendSources(*operands[0], incoming, std::move(condition), sources);
  JoinType type = JoinType::kInner;
  const std::string join_detail = UpperCopy(node.detail);
  if (join_detail == "COMMA") {
    type = JoinType::kCross;
  }
  if (join_detail.find("LEFT") != std::string::npos) {
    type = JoinType::kLeft;
  }
  if (join_detail.find("RIGHT") != std::string::npos) {
    type = JoinType::kRight;
  }
  if (join_detail.find("FULL") != std::string::npos) {
    type = JoinType::kFull;
  }
  Expression join_expression;
  if (on != nullptr && !on->children.empty()) {
    join_expression = VisitExpression(*on->children[0]);
  } else if (using_clause != nullptr) {
    // USING(col, ...) carries no OnClause child; it is an equality join over
    // the shared columns. Dropping it silently would turn the statement into
    // a condition-less join.
    for (const GoogleSqlAstNode* column :
         using_clause->Children("Identifier")) {
      Expression equality = BinaryExpressionExp(
          ColumnValueExp(Identifier(*column)), BinaryOperation::kEquals,
          ColumnValueExp(Identifier(*column)));
      join_expression =
          join_expression
              ? BinaryExpressionExp(std::move(join_expression),
                                    BinaryOperation::kAnd, std::move(equality))
              : std::move(equality);
    }
    if (!join_expression) {
      throw std::runtime_error("GoogleSQL AST: unsupported join USING clause");
    }
  }
  AppendSources(*operands[1], type, std::move(join_expression), sources);
}

// Parses a SelectList node into named projections (shared by SELECT and the
// pipe operators SELECT/EXTEND/AGGREGATE/WINDOW).
// Renders an array Value as a compact JSON array (used by struct literals so
// nested containers keep their shape instead of the ARRAY<T>[...] spelling).
std::string ValueToJsonArray(const Value& value) {
  std::string out = "[";
  bool first = true;
  for (const Value& element : value.ArrayElements()) {
    if (!first) {
      out += ", ";
    }
    first = false;
    if (element.IsNull()) {
      out += "null";
    } else if (element.type == ValueType::kInt64 ||
               element.type == ValueType::kDate) {
      out += std::to_string(element.value.int_value);
    } else if (element.type == ValueType::kDouble) {
      std::ostringstream o;
      o << std::setprecision(17) << element.value.double_value;
      out += o.str();
    } else if (element.IsArray()) {
      out += ValueToJsonArray(element);
    } else {
      std::string text(element.value.varchar_value);
      const bool json_shaped =
          (!text.empty() && ((text.front() == '{' && text.back() == '}') ||
                             (text.front() == '[' && text.back() == ']')));
      out += json_shaped ? text : "\"" + text + "\"";
    }
  }
  out += "]";
  return out;
}

std::vector<NamedExpression> ParseSelectList(
    const GoogleSqlAstNode& select_node) {
  const GoogleSqlAstNode* select_list = select_node.Child("SelectList");
  if (select_list == nullptr) {
    throw std::runtime_error("GoogleSQL AST: SELECT without list");
  }
  std::vector<NamedExpression> projections;
  for (const GoogleSqlAstNode* column : select_list->Children("SelectColumn")) {
    const GoogleSqlAstNode* expression_node = nullptr;
    for (const auto& child : column->children) {
      if (child->kind != "Alias") {
        expression_node = child.get();
        break;
      }
    }
    if (expression_node == nullptr) {
      throw std::runtime_error("GoogleSQL AST: empty column");
    }
    Expression expression = VisitExpression(*expression_node);
    std::string name = Alias(*column);
    if (name.empty() && expression->Type() == TypeTag::kColumnValue) {
      name = expression->AsColumnValue().GetColumnName().name;
    }
    projections.emplace_back(name, std::move(expression));
  }
  return projections;
}

std::shared_ptr<SelectStatement> VisitQuery(
    const GoogleSqlAstNode& query) {
  {
    std::string kids;
    for (const auto& c : query.children) { kids += "[" + c->kind + "]"; }
  }  // NOLINT(misc-no-recursion) // Recursive
                                      // subquery/CTE traversal by design; SQL
                                      // nesting is finite and parser-bounded.
  const GoogleSqlAstNode* select =
      (query.kind == "Select") ? &query : query.Child("Select");
  if (select == nullptr) {
    const GoogleSqlAstNode* set_op =
        (query.kind == "SetOperation") ? &query : query.Child("SetOperation");
    if (set_op == nullptr) {
      for (const auto& child : query.children) {
        if (child->kind == "SetOperation") {
          set_op = child.get();
          break;
        }
      }
    }
    if (set_op != nullptr) {
      std::vector<const GoogleSqlAstNode*> operands;
      for (const auto& child : set_op->children) {
        if (child->kind == "Query" || child->kind == "Select" ||
            child->kind == "SetOperation") {
          operands.push_back(child.get());
        }
      }
      if (!operands.empty()) {
        auto first_stmt = VisitQuery(*operands[0]);
        std::vector<SetOperationKind> operations;
        if (const GoogleSqlAstNode* metadata_list =
                set_op->Child("SetOperationMetadataList")) {
          size_t metadata_index = 0;
          for (const GoogleSqlAstNode* metadata :
               metadata_list->Children("SetOperationMetadata")) {
            const GoogleSqlAstNode* type = metadata->Child("SetOperationType");
            const GoogleSqlAstNode* all_or_distinct =
                metadata->Child("SetOperationAllOrDistinct");
            std::string op = type == nullptr ? "" : UpperCopy(type->detail);
            if (op.empty() && metadata_index == 0) {
              op = UpperCopy(set_op->detail);
            }
            const bool all = all_or_distinct != nullptr &&
                             UpperCopy(all_or_distinct->detail) == "ALL";
            const bool op_all = all || op.find("ALL") != std::string::npos;
            if (op.find("UNION") != std::string::npos) {
              operations.push_back(op_all ? SetOperationKind::kUnionAll
                                          : SetOperationKind::kUnion);
            } else if (op.find("INTERSECT") != std::string::npos) {
              operations.push_back(op_all ? SetOperationKind::kIntersectAll
                                          : SetOperationKind::kIntersect);
            } else if (op.find("EXCEPT") != std::string::npos) {
              operations.push_back(op_all ? SetOperationKind::kExceptAll
                                          : SetOperationKind::kExcept);
            } else if (!op.empty()) {
              throw std::runtime_error(
                  "GoogleSQL AST: unsupported set operation " + op);
            }
            ++metadata_index;
          }
        }
        if (operations.empty()) {
          const std::string op = UpperCopy(set_op->detail);
          operations.push_back(op.find("INTERSECT") != std::string::npos
                                   ? (op.find("ALL") != std::string::npos
                                          ? SetOperationKind::kIntersectAll
                                          : SetOperationKind::kIntersect)
                               : op.find("EXCEPT") != std::string::npos
                                   ? (op.find("ALL") != std::string::npos
                                          ? SetOperationKind::kExceptAll
                                          : SetOperationKind::kExcept)
                                   : (op.find("ALL") != std::string::npos
                                          ? SetOperationKind::kUnionAll
                                          : SetOperationKind::kUnion));
        }
        while (operations.size() < operands.size() - 1) {
          operations.push_back(operations.back());
        }
        std::vector<SetOperationMatch> matches;
        if (const GoogleSqlAstNode* metadata_list =
                set_op->Child("SetOperationMetadataList")) {
          for (const GoogleSqlAstNode* metadata :
               metadata_list->Children("SetOperationMetadata")) {
            SetOperationMatch match;
            const GoogleSqlAstNode* match_mode =
                metadata->Child("SetOperationColumnMatchMode");
            const GoogleSqlAstNode* propagation_mode =
                metadata->Child("SetOperationColumnPropagationMode");
            if (match_mode != nullptr) {
              // The pinned parser emits an identical node for CORRESPONDING
              // and BY NAME; only the source span differs ("BY NAME" is 7
              // characters, "CORRESPONDING" longer).  A propagation-mode
              // node marks the INNER variant, which intersects columns.
              const size_t span = match_mode->end > match_mode->start
                                      ? match_mode->end - match_mode->start
                                      : 0;
              // Propagation mode carries FULL (4 chars) or INNER (5); it
              // selects between the union and the intersection of names.
              size_t propagation_span = 0;
              if (propagation_mode != nullptr &&
                  propagation_mode->end > propagation_mode->start) {
                propagation_span =
                    propagation_mode->end - propagation_mode->start;
              }
              const GoogleSqlAstNode* by_list =
                  match_mode->Child("ExpressionList");
              if (by_list == nullptr) {
                by_list = metadata->Child("ExpressionList");
              }
              if (span > 0 && span <= 8) {
                match.by_name = true;
                if (propagation_span > 4) {
                  match.corresponding = true;  // INNER: intersect columns
                }
              } else {
                match.corresponding = true;
              }
              if (by_list != nullptr) {
                for (const GoogleSqlAstNode* identifier :
                     by_list->Children("Identifier")) {
                  match.columns.push_back(Identifier(*identifier));
                }
              }
            }
            matches.push_back(std::move(match));
          }
        }
        while (matches.size() < operands.size() - 1) {
          matches.push_back(matches.empty() ? SetOperationMatch{}
                                            : matches.back());
        }
        for (size_t i = 1; i < operands.size(); ++i) {
          const SetOperationKind operation = i - 1 < operations.size()
                                                 ? operations[i - 1]
                                                 : SetOperationKind::kUnionAll;
          SetOperationMatch match =
              i - 1 < matches.size() ? matches[i - 1] : SetOperationMatch{};
          first_stmt->AddSetOperation(operation, VisitQuery(*operands[i]),
                                      std::move(match));
        }
        // ORDER BY/LIMIT/OFFSET are attached to the Query parent of a set
        // operation. Preserve them on the synthetic head statement so the
        // relational executor can apply them after all UNION ALL branches.
        std::vector<SelectStatement::OrderByTerm> order_by;
        if (const GoogleSqlAstNode* order = query.Child("OrderBy")) {
          for (const GoogleSqlAstNode* term :
               order->Children("OrderingExpression")) {
            WindowOrderTerm parsed = ParseOrderingTerm(term);
            if (parsed.expression) {
              order_by.push_back({std::move(parsed.expression),
                                  parsed.ascending, parsed.nulls_first});
            }
          }
        }
        if (!order_by.empty()) {
          first_stmt->SetOrderBy(std::move(order_by));
        }
        std::optional<size_t> limit;
        size_t offset = 0;
        if (const GoogleSqlAstNode* limit_offset = query.Child("LimitOffset")) {
          if (const GoogleSqlAstNode* limit_node =
                  limit_offset->Child("Limit")) {
            if (const GoogleSqlAstNode* value =
                    limit_node->Child("IntLiteral")) {
              limit = static_cast<size_t>(ParseUnsignedLiteral(*value));
            }
          }
          for (const auto& child : limit_offset->children) {
            if (child->kind == "IntLiteral") {
              offset = ParseUnsignedLiteral(*child);
            }
          }
        }
        if (limit.has_value()) {
          first_stmt->SetLimit(limit);
        }
        first_stmt->SetOffset(offset);
        // The WITH clause belongs to the query wrapper around a set
        // operation, not to its first SELECT operand.  Preserve it on the
        // synthetic head so CTE visibility and materialization are identical
        // for `WITH cte AS (...) SELECT ... UNION ALL SELECT ...`.
        if (const GoogleSqlAstNode* with = query.Child("WithClause")) {
          for (const GoogleSqlAstNode* entry :
               with->Children("WithClauseEntry")) {
            const GoogleSqlAstNode* aliased = entry->Child("AliasedQuery");
            if (aliased == nullptr) {
              continue;
            }
            const GoogleSqlAstNode* name = aliased->Child("Identifier");
            const GoogleSqlAstNode* nested = aliased->Child("Query");
            if (name != nullptr && nested != nullptr) {
              first_stmt->AddWithQuery(Identifier(*name), VisitQuery(*nested));
            }
          }
        }
        return first_stmt;
      }
    }
    // Pipe syntax: `FROM t |> WHERE ... |> AGGREGATE ... |> ...`.  The base
    // is a FromQuery; each pipe operator maps onto a standard clause of the
    // same statement.
    const GoogleSqlAstNode* from_query = nullptr;
    const GoogleSqlAstNode* base_query = nullptr;
    for (const auto& child : query.children) {
      if (child->kind == "FromQuery") {
        from_query = child.get();
        break;
      }
      if (child->kind == "Query" || child->kind == "Select") {
        // Parenthesized-query base: `(SELECT ...) |> ...`.
        base_query = child.get();
        break;
      }
    }
    if (from_query != nullptr || base_query != nullptr) {
      std::shared_ptr<SelectStatement> statement;
      if (base_query != nullptr) {
        statement = VisitQuery(*base_query);
      } else {
        statement = std::make_shared<SelectStatement>(
            std::vector<NamedExpression>{}, std::vector<std::string>{},
            nullptr);
        statement->SetSelectList({NamedExpression("", ColumnValueExp("*"))});
        std::vector<SelectSource> sources;
        if (const GoogleSqlAstNode* from = from_query->Child("FromClause")) {
          for (const auto& child : from->children) {
            AppendSources(*child, JoinType::kCross, nullptr, &sources);
          }
        }
        statement->SetSources(std::move(sources));
        statement->MarkComplex();
      }

      // Walk the pipe operators in source order.  They are siblings of the
      // FromQuery under the Query node.
      for (const auto& child : query.children) {
        const GoogleSqlAstNode& pipe = *child;
        if (&pipe == from_query || child.get() == base_query ||
            pipe.kind == "Query" || pipe.kind == "Select" ||
            pipe.kind == "WithClause") {
          continue;
        }
        if (!pipe.kind.starts_with("Pipe")) {
          continue;
        }
        const std::string op = pipe.kind.substr(4);
        if (op == "Where") {
          const GoogleSqlAstNode* where_clause = pipe.Child("WhereClause");
          if (where_clause != nullptr && !where_clause->children.empty()) {
            Expression condition = VisitExpression(*where_clause->children[0]);
            // A predicate over window functions must run after the window
            // pass: route it through QUALIFY.
            std::function<bool(const Expression&)> has_window =
                [&](const Expression& e) -> bool {
              if (!e) {
                return false;
              }
              if (e->Type() == TypeTag::kWindowFunctionExp) {
                return true;
              }
              for (const Expression& c : ExpressionChildren(e)) {
                if (has_window(c)) {
                  return true;
                }
              }
              return false;
            };
            if (has_window(condition)) {
              if (statement->Qualify()) {
                condition = BinaryExpressionExp(statement->Qualify(),
                                                BinaryOperation::kAnd,
                                                std::move(condition));
              }
              statement->SetQualify(std::move(condition));
              continue;
            }
            // A predicate referencing aggregate output aliases after
            // |> AGGREGATE behaves as HAVING; inline the aliases first.
            bool references_alias = false;
            if (!statement->GroupBy().empty()) {
              for (const NamedExpression& item : statement->SelectList()) {
                if (item.name.empty()) {
                  continue;
                }
                std::function<bool(const Expression&)> touches =
                    [&](const Expression& e) -> bool {
                  if (!e || e->Type() != TypeTag::kColumnValue) {
                    return false;
                  }
                  const std::string& n =
                      e->AsColumnValue().GetColumnName().name;
                  return n.size() == item.name.size() &&
                         std::equal(
                             n.begin(), n.end(), item.name.begin(),
                             [](char x, char y) {
                               return std::tolower(
                                          static_cast<unsigned char>(x)) ==
                                      std::tolower(
                                          static_cast<unsigned char>(y));
                             });
                };
                std::function<void(Expression&)> inline_alias =
                    [&](Expression& e) {
                      if (!e) {
                        return;
                      }
                      if (touches(e)) {
                        references_alias = true;
                        e = item.expression;
                        return;
                      }
                      std::vector<Expression> children = ExpressionChildren(e);
                      bool changed = false;
                      for (Expression& c : children) {
                        inline_alias(c);
                        changed = true;
                      }
                      if (changed) {
                        e = WithExpressionChildren(e, std::move(children));
                      }
                    };
                inline_alias(condition);
              }
            }
            if (references_alias) {
              if (statement->Having()) {
                condition = BinaryExpressionExp(statement->Having(),
                                                BinaryOperation::kAnd,
                                                std::move(condition));
              }
              statement->SetHaving(std::move(condition));
            } else if (statement->WhereClause()) {
              condition = BinaryExpressionExp(statement->WhereClause(),
                                              BinaryOperation::kAnd,
                                              std::move(condition));
              statement->SetWhereClause(std::move(condition));
            } else {
              statement->SetWhereClause(std::move(condition));
            }
          }
        } else if (op == "Select" || op == "Aggregate" || op == "Extend" ||
                   op == "Window") {
          const GoogleSqlAstNode* inner = pipe.Child("Select");
          if (inner == nullptr) {
            continue;
          }
          std::vector<NamedExpression> parsed = ParseSelectList(*inner);
          if (op == "Extend" || op == "Window") {
            // Keep existing columns and append computed ones.
            auto& list = statement->SelectList();
            for (auto& item : parsed) {
              list.push_back(std::move(item));
            }
          } else {
            statement->SetSelectList(std::move(parsed));
          }
          if (op == "Aggregate") {
            std::vector<Expression> group_by;
            std::vector<SelectStatement::OrderByTerm> agg_order;
            if (const GoogleSqlAstNode* group = inner->Child("GroupBy")) {
              const bool and_order =
                  UpperCopy(group->detail).find("AND_ORDER_BY") !=
                      std::string::npos ||
                  UpperCopy(group->detail).find("and_order_by") !=
                      std::string::npos;
              for (const GoogleSqlAstNode* item :
                   group->Children("GroupingItem")) {
                if (item->children.empty()) {
                  continue;
                }
                group_by.push_back(VisitExpression(*item->children[0]));
                const GoogleSqlAstNode* item_order =
                    item->Child("GroupingItemOrder");
                if (and_order && item_order != nullptr) {
                  // Ordering applies to the group key itself; the direction
                  // lives in the node detail ("ASC"/"DESC").
                  agg_order.push_back(
                      {group_by.back(),
                       UpperCopy(item_order->detail).find("DESC") ==
                           std::string::npos,
                       std::nullopt});
                }
              }
            }
            // The aggregate output carries the grouping columns first, then
            // the aggregate expressions.
            std::vector<NamedExpression> with_keys;
            auto& list = statement->SelectList();
            for (const Expression& key : group_by) {
              bool already_projected = false;
              const std::string key_text = key->ToString();
              for (const NamedExpression& item : list) {
                if (item.expression &&
                    item.expression->ToString() == key_text) {
                  already_projected = true;
                  break;
                }
              }
              if (!already_projected) {
                std::string key_name;
                if (key->Type() == TypeTag::kColumnValue) {
                  key_name = key->AsColumnValue().GetColumnName().name;
                } else {
                  key_name = key_text;
                }
                with_keys.emplace_back(key_name, key);
              }
            }
            for (auto& item : list) {
              with_keys.push_back(std::move(item));
            }
            statement->SetSelectList(std::move(with_keys));
            statement->SetGroupBy(std::move(group_by));
            if (!agg_order.empty()) {
              statement->SetOrderBy(std::move(agg_order));
            }
          }
        } else if (op == "OrderBy") {
          const GoogleSqlAstNode* order = pipe.Child("OrderBy");
          if (order != nullptr) {
            std::vector<SelectStatement::OrderByTerm> order_by;
            for (const GoogleSqlAstNode* term :
                 order->Children("OrderingExpression")) {
              WindowOrderTerm parsed = ParseOrderingTerm(term);
              if (parsed.expression) {
                order_by.push_back({std::move(parsed.expression),
                                    parsed.ascending, parsed.nulls_first});
              }
            }
            statement->SetOrderBy(std::move(order_by));
          }
        } else if (op == "LimitOffset") {
          const GoogleSqlAstNode* limit_offset = pipe.Child("LimitOffset");
          if (limit_offset != nullptr) {
            std::optional<size_t> limit;
            size_t offset = 0;
            if (const GoogleSqlAstNode* limit_node =
                    limit_offset->Child("Limit")) {
              if (const GoogleSqlAstNode* value =
                      limit_node->Child("IntLiteral")) {
                limit = static_cast<size_t>(ParseUnsignedLiteral(*value));
              }
            }
            for (const auto& c : limit_offset->children) {
              if (c->kind == "IntLiteral") {
                offset = ParseUnsignedLiteral(*c);
              }
            }
            statement->SetLimit(limit);
            statement->SetOffset(offset);
          }
        } else if (op == "Distinct") {
          statement->MarkDistinct();
        } else if (op == "Assert") {
          // ASSERT cond: rows failing the predicate are runtime errors;
          // approximate as a filter so passing inputs still work.
          if (!pipe.children.empty() &&
              pipe.children.front()->kind != "Location") {
            Expression condition = VisitExpression(*pipe.children.front());
            if (statement->WhereClause()) {
              condition = BinaryExpressionExp(statement->WhereClause(),
                                              BinaryOperation::kAnd,
                                              std::move(condition));
            }
            statement->SetWhereClause(std::move(condition));
          }
        } else if (op == "Join") {
          // FROM a |> JOIN b ON/USING ...  The right operand sits inside a
          // Join node whose LHS is a placeholder.
          std::vector<SelectSource> join_sources;
          const GoogleSqlAstNode* join_node = pipe.Child("Join");
          const GoogleSqlAstNode& join_root =
              join_node != nullptr ? *join_node : pipe;
          JoinType jt = JoinType::kInner;
          const std::string detail =
              UpperCopy(pipe.detail.empty() ? join_root.detail : pipe.detail);
          if (detail.find("LEFT") != std::string::npos) {
            jt = JoinType::kLeft;
          } else if (detail.find("RIGHT") != std::string::npos) {
            jt = JoinType::kRight;
          } else if (detail.find("FULL") != std::string::npos) {
            jt = JoinType::kFull;
          } else if (detail.find("CROSS") != std::string::npos ||
                     detail.find("COMMA") != std::string::npos) {
            jt = JoinType::kCross;
          }
          Expression on;
          for (const auto& c : join_root.children) {
            if (c->kind == "OnClause") {
              if (!c->children.empty()) {
                on = VisitExpression(*c->children[0]);
              }
            } else if (c->kind.starts_with("Using")) {
              Expression chain;
              for (const GoogleSqlAstNode* column : c->Children("Identifier")) {
                Expression equality =
                    BinaryExpressionExp(ColumnValueExp(Identifier(*column)),
                                        BinaryOperation::kEquals,
                                        ColumnValueExp(Identifier(*column)));
                chain = chain ? BinaryExpressionExp(std::move(chain),
                                                    BinaryOperation::kAnd,
                                                    std::move(equality))
                              : std::move(equality);
              }
              on =
                  on ? BinaryExpressionExp(std::move(on), BinaryOperation::kAnd,
                                           std::move(chain))
                     : std::move(chain);
            }
          }
          for (const auto& c : join_root.children) {
            if (c->kind == "TablePathExpression" ||
                c->kind == "TableSubquery" || c->kind == "Join" ||
                c->kind == "UnnestExpression") {
              AppendSources(*c, jt, std::move(on), &join_sources);
              break;
            }
          }
          std::vector<SelectSource> sources_mut = statement->Sources();
          for (auto& js : join_sources) {
            sources_mut.push_back(std::move(js));
          }
          statement->SetSources(std::move(sources_mut));
          statement->MarkComplex();
        } else if (op == "SetOperation") {
          // |> UNION ALL (query), |> INTERSECT DISTINCT (query), ...
          // The operation keyword and ALL/DISTINCT live in the metadata.
          const GoogleSqlAstNode* nested_query = nullptr;
          SetOperationKind kind = SetOperationKind::kUnionAll;
          bool all = false;
          std::string op_text;
          for (const auto& c : pipe.children) {
            if (c->kind == "SetOperationMetadata" ||
                c->kind == "SetOperationMetadataList") {
              // Detail strings are empty; the token length identifies the
              // keyword ("UNION"=5, "EXCEPT"=6, "INTERSECT"=8, "ALL"=3).
              auto token_len = [](const GoogleSqlAstNode* n) -> size_t {
                return n != nullptr && n->end > n->start ? n->end - n->start
                                                         : 0;
              };
              const GoogleSqlAstNode* metadata =
                  c->kind == "SetOperationMetadataList"
                      ? (c->children.empty() ? nullptr
                                             : c->children.front().get())
                      : c.get();
              const size_t type_len = token_len(
                  metadata ? metadata->Child("SetOperationType") : nullptr);
              if (type_len == 8) {
                op_text = "INTERSECT";
              } else if (type_len == 6) {
                op_text = "EXCEPT";
              } else if (type_len == 5) {
                op_text = "UNION";
              }
              const size_t all_len =
                  token_len(metadata != nullptr
                                ? metadata->Child("SetOperationAllOrDistinct")
                                : nullptr);
              if (all_len == 3) {
                all = true;
              }
              continue;
            }
            if (c->kind == "Query" || c->kind == "Select" ||
                c->kind == "SetOperation") {
              if (nested_query == nullptr) {
                nested_query = c.get();
              }
            }
          }
          if (op_text.find("INTERSECT") != std::string::npos) {
            kind = all ? SetOperationKind::kIntersectAll
                       : SetOperationKind::kIntersect;
          } else if (op_text.find("EXCEPT") != std::string::npos) {
            kind =
                all ? SetOperationKind::kExceptAll : SetOperationKind::kExcept;
          } else {
            kind = all ? SetOperationKind::kUnionAll : SetOperationKind::kUnion;
          }
          if (nested_query != nullptr) {
            SetOperationMatch match;
            for (const auto& c : pipe.children) {
              const GoogleSqlAstNode* match_mode =
                  c->kind == "SetOperationMetadata"
                      ? c->Child("SetOperationColumnMatchMode")
                      : nullptr;
              if (match_mode == nullptr) {
                continue;
              }
              const size_t span = match_mode->end > match_mode->start
                                      ? match_mode->end - match_mode->start
                                      : 0;
              if (span > 0 && span <= 8) {
                match.by_name = true;
              } else {
                match.corresponding = true;
              }
            }
            statement->AddSetOperation(kind, VisitQuery(*nested_query),
                                       std::move(match));
          }
        } else {
          throw std::runtime_error("GoogleSQL AST: unsupported pipe operator " +
                                   pipe.kind);
        }
      }

      // WITH clauses hang off the top-level query node. A "(recursive)"
      // detail marks WITH RECURSIVE: every CTE in that clause may reference
      // itself and is evaluated iteratively by the relational executor.
      if (const GoogleSqlAstNode* with = query.Child("WithClause")) {
        const bool recursive =
            Lower(with->detail).find("recursive") != std::string::npos;
        for (const GoogleSqlAstNode* entry :
             with->Children("WithClauseEntry")) {
          const GoogleSqlAstNode* aliased = entry->Child("AliasedQuery");
          if (aliased == nullptr) {
            continue;
          }
          const GoogleSqlAstNode* name = aliased->Child("Identifier");
          const GoogleSqlAstNode* nested = aliased->Child("Query");
          if (name != nullptr && nested != nullptr) {
            if (recursive) {
              statement->AddRecursiveWithQuery(Identifier(*name),
                                               VisitQuery(*nested));
            } else {
              statement->AddWithQuery(Identifier(*name), VisitQuery(*nested));
            }
          }
        }
      }
      return statement;
    }
    throw std::runtime_error("GoogleSQL AST: query without SELECT");
  }
  const GoogleSqlAstNode* select_list = select->Child("SelectList");
  if (select_list == nullptr) {
    throw std::runtime_error("GoogleSQL AST: SELECT without list");
  }

  // Named windows must resolve before select-list expressions are built.
  // The WindowClause hangs off the Select node, next to the FromClause.
  struct NamedWindowsScope {
    std::unordered_map<std::string, NamedWindowParts> previous;
    ~NamedWindowsScope() { t_named_windows.swap(previous); }
  } named_windows_scope;
  const GoogleSqlAstNode* window_clause =
      select->Child("WindowClause") != nullptr ? select->Child("WindowClause")
                                               : query.Child("WindowClause");
  if (window_clause != nullptr) {
    for (const GoogleSqlAstNode* definition :
         window_clause->Children("WindowDefinition")) {
      const GoogleSqlAstNode* name_node = definition->Child("Identifier");
      const GoogleSqlAstNode* spec = definition->Child("WindowSpecification");
      if (name_node == nullptr || spec == nullptr) {
        continue;
      }
      NamedWindowParts parts;
      if (const GoogleSqlAstNode* base = spec->Child("Identifier")) {
        const auto found = t_named_windows.find(Identifier(*base));
        if (found != t_named_windows.end()) {
          parts = found->second;
        }
      }
      NamedWindowParts own = ParseWindowSpecification(*spec);
      if (!own.partition_by.empty()) {
        parts.partition_by = std::move(own.partition_by);
      }
      if (!own.order_by.empty()) {
        parts.order_by = std::move(own.order_by);
      }
      if (own.has_frame) {
        parts.frame_unit = own.frame_unit;
        parts.frame_start = own.frame_start;
        parts.frame_end = own.frame_end;
        parts.has_frame = true;
      }
      if (own.exclusion != WindowFrameExclusion::kNone) {
        parts.exclusion = own.exclusion;
      }
      t_named_windows[Identifier(*name_node)] = std::move(parts);
    }
  }

  std::vector<NamedExpression> projections = ParseSelectList(*select);

  const std::string upper_select_detail = UpperCopy(select->detail);
  const GoogleSqlAstNode* select_as = select->Child("SelectAs");
  const GoogleSqlAstNode* as_struct = select->Child("AsStruct");
  const bool is_as_struct =
      upper_select_detail.find("STRUCT") != std::string::npos ||
      (select_as != nullptr &&
       UpperCopy(select_as->detail).find("STRUCT") != std::string::npos) ||
      as_struct != nullptr;
  if (!is_as_struct &&
      (select_as != nullptr ||
       (upper_select_detail.find("AS_MODE=") != std::string::npos &&
        upper_select_detail.find("AS_MODE=VALUE") == std::string::npos))) {
    std::string proto_str;
    bool all_const = true;
    for (size_t i = 0; i < projections.size(); ++i) {
      std::string fname = projections[i].name.empty()
                              ? ("f" + std::to_string(i + 1))
                              : projections[i].name;
      if (projections[i].expression->Type() == TypeTag::kConstantValue) {
        if (!proto_str.empty()) {
          proto_str += " ";
        }
        proto_str += fname + ": ";
        const Value& v =
            projections[i].expression->AsConstantValue().GetValue();
        proto_str += v.AsString();
      } else if (projections[i].expression->Type() == TypeTag::kArrayExp) {
        const auto& arr = projections[i].expression->AsArrayExpression();
        for (const auto& elem : arr.Elements()) {
          if (elem->Type() == TypeTag::kConstantValue) {
            if (!proto_str.empty()) {
              proto_str += " ";
            }
            proto_str +=
                fname + ": " + elem->AsConstantValue().GetValue().AsString();
          } else {
            all_const = false;
          }
        }
      } else {
        all_const = false;
      }
    }
    if (all_const) {
      projections = {
          NamedExpression("", ConstantValueExp(Value(std::move(proto_str))))};
    }
  }
  if (is_as_struct && projections.size() > 1) {
    // Collapse the multi-column projection into one STRUCT value rendered as
    // JSON text: `SELECT AS STRUCT a, b` yields a single struct column.
    std::vector<Expression> args;
    for (NamedExpression& item : projections) {
      args.push_back(item.expression);
      args.push_back(ConstantValueExp(Value(std::move(item.name))));
    }
    projections.clear();
    projections.push_back(
        NamedExpression("", FunctionCallExp("__struct", std::move(args))));
  }

  std::vector<SelectSource> sources;
  std::vector<std::string> tables;
  if (const GoogleSqlAstNode* from = select->Child("FromClause")) {
    for (const auto& child : from->children) {
      AppendSources(*child, JoinType::kCross, nullptr, &sources);
    }
    for (const SelectSource& source : sources) {
      if (!source.table.empty()) {
        tables.push_back(source.table);
      }
    }
  }

  Expression where;
  if (const GoogleSqlAstNode* clause = select->Child("WhereClause")) {
    if (!clause->children.empty()) {
      where = VisitExpression(*clause->children[0]);
    }
  }

  std::vector<SelectStatement::OrderByTerm> order_by;
  if (const GoogleSqlAstNode* order = query.Child("OrderBy")) {
    for (const GoogleSqlAstNode* term : order->Children("OrderingExpression")) {
      WindowOrderTerm parsed = ParseOrderingTerm(term);
      if (!parsed.expression) {
        continue;
      }
      order_by.push_back(
          {std::move(parsed.expression), parsed.ascending, parsed.nulls_first});
    }
  }

  std::optional<size_t> limit;
  size_t offset = 0;
  if (const GoogleSqlAstNode* limit_offset = query.Child("LimitOffset")) {
    if (const GoogleSqlAstNode* limit_node = limit_offset->Child("Limit")) {
      if (const GoogleSqlAstNode* value = limit_node->Child("IntLiteral")) {
        limit = static_cast<size_t>(ParseUnsignedLiteral(*value));
      }
    }
    for (const auto& child : limit_offset->children) {
      if (child->kind == "IntLiteral") {
        offset = ParseUnsignedLiteral(*child);
      }
    }
  }

  auto statement = std::make_shared<SelectStatement>(
      std::move(projections), std::move(tables), std::move(where),
      std::move(order_by), limit.value_or(0), offset,
      select->detail.find("distinct=true") != std::string::npos);
  statement->SetLimit(limit);
  statement->SetSources(std::move(sources));

  if (const GoogleSqlAstNode* group = select->Child("GroupBy")) {
    std::vector<Expression> expressions;
    std::vector<std::vector<Expression>> grouping_sets;
    bool has_grouping_extension = false;
    for (const GoogleSqlAstNode* item : group->Children("GroupingItem")) {
      const GoogleSqlAstNode* content =
          item->children.empty() ? nullptr : item->children[0].get();
      while (content != nullptr && content->kind == "Location" &&
             !content->children.empty()) {
        content = content->children[0].get();
      }
      if (content == nullptr || content->kind == "Location") {
        continue;
      }
      if (content->kind == "Rollup" || content->kind == "Cube") {
        has_grouping_extension = true;
        std::vector<Expression> columns;
        for (const auto& child : content->children) {
          if (child->kind == "Location") {
            continue;
          }
          columns.push_back(VisitExpression(*child));
        }
        // ROLLUP(a, b): prefixes (a,b), (a), (); CUBE: every subset.
        const size_t n = columns.size();
        for (size_t mask = 0; mask < (size_t{1} << n); ++mask) {
          if (content->kind == "Rollup") {
            // Keep only the prefix masks: bits above the lowest zero must be
            // set (mask+1 is a power of two => prefix pattern).
            if (((mask + 1) & mask) != 0) {
              continue;
            }
          }
          std::vector<Expression> set;
          for (size_t i = 0; i < n; ++i) {
            if ((mask >> i) & size_t{1}) {
              set.push_back(columns[i]);
            }
          }
          grouping_sets.push_back(std::move(set));
        }
      } else if (content->kind == "GroupingSetList") {
        has_grouping_extension = true;
        for (const GoogleSqlAstNode* set_node :
             content->Children("GroupingSet")) {
          std::vector<Expression> set;
          for (const auto& child : set_node->children) {
            if (child->kind == "Location") {
              continue;
            }
            if (child->kind == "StructConstructorWithParens" ||
                child->kind == "ExpressionList" ||
                child->kind == "StructConstructorWithKeyword") {
              // `(a, b)` groups several columns into one set.
              for (const auto& inner : child->children) {
                if (inner->kind == "Location") {
                  continue;
                }
                set.push_back(VisitExpression(*inner));
              }
              continue;
            }
            set.push_back(VisitExpression(*child));
          }
          grouping_sets.push_back(std::move(set));
        }
      } else {
        expressions.push_back(VisitExpression(*content));
        continue;
      }
    }
    // Resolve ordinals and select-list aliases inside grouping sets
    // (GROUPING SETS((1, b), (ub)) refers to projection items).
    {
      const auto resolve = [&projections](const Expression& e) -> Expression {
        if (!e) {
          return e;
        }
        if (e->Type() == TypeTag::kColumnValue) {
          const std::string& name = e->AsColumnValue().GetColumnName().name;
          for (const NamedExpression& item : projections) {
            if (!item.name.empty() &&
                std::equal(name.begin(), name.end(), item.name.begin(),
                           [](char a, char b) {
                             return std::tolower(
                                        static_cast<unsigned char>(a)) ==
                                    std::tolower(static_cast<unsigned char>(b));
                           })) {
              return item.expression;
            }
          }
          return e;
        }
        if (e->Type() == TypeTag::kConstantValue &&
            e->AsConstantValue().GetValue().type == ValueType::kInt64) {
          const int64_t ordinal =
              e->AsConstantValue().GetValue().value.int_value;
          if (ordinal >= 1 &&
              ordinal <= static_cast<int64_t>(projections.size())) {
            return projections[static_cast<size_t>(ordinal - 1)].expression;
          }
        }
        return e;
      };
      for (auto& set : grouping_sets) {
        for (auto& e : set) {
          e = resolve(e);
        }
      }
    }
    statement->SetGroupBy(std::move(expressions));
    if (has_grouping_extension) {
      // The flattened universe of groupable columns must live in GroupBy()
      // so downstream planning treats the query as an aggregate query.
      std::vector<Expression> flat = statement->GroupBy();
      auto contains = [&](const Expression& e) {
        for (const Expression& existing : flat) {
          if (existing.get() == e.get()) {
            return true;
          }
        }
        return false;
      };
      for (const auto& set : grouping_sets) {
        for (const Expression& column : set) {
          if (!contains(column)) {
            flat.push_back(column);
          }
        }
      }
      statement->SetGroupBy(std::move(flat));
      if (grouping_sets.empty()) {
        grouping_sets.emplace_back();
      }
      statement->SetGroupingSets(std::move(grouping_sets));
    }
  }
  if (const GoogleSqlAstNode* having = select->Child("Having")) {
    if (!having->children.empty()) {
      statement->SetHaving(VisitExpression(*having->children[0]));
    }
  }
  const GoogleSqlAstNode* qualify = select->Child("Qualify") != nullptr
                                        ? select->Child("Qualify")
                                        : query.Child("Qualify");
  if (qualify != nullptr) {
    if (!qualify->children.empty()) {
      statement->SetQualify(VisitExpression(*qualify->children[0]));
    }
  }
  if (const GoogleSqlAstNode* with = query.Child("WithClause")) {
    // "(recursive)" marks WITH RECURSIVE: every CTE in the clause may
    // reference itself and is evaluated iteratively by the relational
    // executor.
    const bool recursive =
        Lower(with->detail).find("recursive") != std::string::npos;
    for (const GoogleSqlAstNode* entry : with->Children("WithClauseEntry")) {
      const GoogleSqlAstNode* aliased = entry->Child("AliasedQuery");
      if (aliased == nullptr) {
        continue;
      }
      const GoogleSqlAstNode* name = aliased->Child("Identifier");
      const GoogleSqlAstNode* nested = aliased->Child("Query");
      if (name != nullptr && nested != nullptr) {
        if (recursive) {
          const std::string cte_name = Identifier(*name);
          statement->AddRecursiveWithQuery(cte_name, VisitQuery(*nested));
          // `WITH DEPTH [AS col] BETWEEN lo AND hi` caps the iteration
          // count and exposes each row's recursion depth as a column.
          if (const GoogleSqlAstNode* modifiers =
                  aliased->Child("AliasedQueryModifiers")) {
            if (const GoogleSqlAstNode* depth_modifier =
                    modifiers->Child("RecursionDepthModifier")) {
              RecursiveDepthSpec spec;
              if (const GoogleSqlAstNode* alias =
                      depth_modifier->Child("Alias")) {
                if (const GoogleSqlAstNode* column =
                        alias->Child("Identifier")) {
                  spec.column = Identifier(*column);
                }
              }
              const auto bounds = depth_modifier->Children("IntOrUnbounded");
              auto bound_value = [&](size_t index, int64_t fallback) {
                if (index >= bounds.size()) {
                  return fallback;
                }
                for (const auto& child : bounds[index]->children) {
                  if (child->kind == "IntLiteral") {
                    return static_cast<int64_t>(ParseUnsignedLiteral(*child));
                  }
                }
                return fallback;  // UNBOUNDED
              };
              spec.lower = bound_value(0, 0);
              spec.upper = bound_value(1, std::numeric_limits<int64_t>::max());
              statement->SetRecursiveDepth(cte_name, std::move(spec));
            }
          }
        } else {
          statement->AddWithQuery(Identifier(*name), VisitQuery(*nested));
        }
      }
    }
  }
  // Phase 8 routing: plain multi-table FROM lists (cross/inner joins, with
  // or without table aliases) stay on the cost-based optimizer path; the
  // engine folds INNER ON conditions into the WHERE conjunction. Only
  // features the optimizer cannot represent yet force the relational
  // executor: FROM-subqueries and outer joins.
  for (const SelectSource& source : statement->Sources()) {
    if (source.query || source.join_type == JoinType::kLeft ||
        source.join_type == JoinType::kRight ||
        source.join_type == JoinType::kFull) {
      statement->MarkComplex();
    }
  }
  for (const NamedExpression& projection : statement->SelectList()) {
    if (NeedsRelationalEvaluation(projection.expression)) {
      statement->MarkComplex();
    }
  }
  if (NeedsRelationalEvaluation(statement->WhereClause()) ||
      NeedsRelationalEvaluation(statement->Having())) {
    statement->MarkComplex();
  }
  return statement;
}

ValueType ColumnType(const GoogleSqlAstNode& definition) {
  const GoogleSqlAstNode* schema = definition.Child("SimpleColumnSchema");
  const GoogleSqlAstNode* path =
      schema != nullptr ? schema->Child("PathExpression") : nullptr;
  if (path == nullptr) {
    throw std::runtime_error("GoogleSQL AST: column type missing");
  }
  const std::string type = Lower(Path(*path));
  if (type == "int" || type == "int64" || type == "integer" ||
      type == "bigint" || type == "bool" || type == "boolean") {
    return ValueType::kInt64;
  }
  if (type == "numeric" || type == "decimal" || type == "double" ||
      type == "float" || type == "float64") {
    return ValueType::kDouble;
  }
  if (type == "date") {
    return ValueType::kDate;
  }
  if (type == "string" || type == "varchar" || type == "char" ||
      type == "timestamp" || type == "datetime") {
    return ValueType::kVarChar;
  }
  // Proto/enum/bytes column types are tolerated as VARCHAR so mixed-type
  // CREATE TABLE statements succeed; the proto columns carry opaque text.
  return ValueType::kVarChar;
}

std::unique_ptr<Statement> VisitCreate(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    throw std::runtime_error("GoogleSQL AST: bad CREATE");
  }
  const GoogleSqlAstNode* elements = root.Child("TableElementList");
  if (elements != nullptr) {
    std::vector<Column> columns;
    for (const GoogleSqlAstNode* definition :
         elements->Children("ColumnDefinition")) {
      const GoogleSqlAstNode* name = definition->Child("Identifier");
      if (name == nullptr) {
        throw std::runtime_error("GoogleSQL AST: unnamed column");
      }
      columns.emplace_back(Identifier(*name), ColumnType(*definition));
    }
    // `CREATE TABLE t (cols) AS SELECT ...`: schema plus initial data.
    if (const GoogleSqlAstNode* query = root.Child("Query")) {
      auto statement = VisitQuery(*query);
      auto created = std::make_unique<CreateTableStatement>(Path(*path),
                                                            std::move(columns));
      created->SetAsQuery(std::move(statement));
      return created;
    }
    return std::make_unique<CreateTableStatement>(Path(*path),
                                                  std::move(columns));
  }
  const GoogleSqlAstNode* query = root.Child("Query");
  if (query != nullptr) {
    auto statement = VisitQuery(*query);
    return std::make_unique<CreateTableStatement>(Path(*path),
                                                  std::move(statement));
  }
  throw std::runtime_error("GoogleSQL AST: bad CREATE");
}

std::unique_ptr<Statement> VisitInsert(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    throw std::runtime_error("GoogleSQL AST: INSERT table missing");
  }
  std::vector<std::string> columns;
  if (const GoogleSqlAstNode* list = root.Child("ColumnList")) {
    for (const GoogleSqlAstNode* name : list->Children("Identifier")) {
      columns.push_back(Identifier(*name));
    }
  }
  if (const GoogleSqlAstNode* query = root.Child("Query")) {
    return std::make_unique<InsertStatement>(Path(*path), VisitQuery(*query),
                                             std::move(columns));
  }
  std::vector<std::vector<Expression>> rows;
  const GoogleSqlAstNode* row_list = root.Child("InsertValuesRowList");
  if (row_list == nullptr) {
    throw std::runtime_error("GoogleSQL AST: INSERT VALUES required");
  }
  for (const GoogleSqlAstNode* row : row_list->Children("InsertValuesRow")) {
    std::vector<Expression> values;
    for (const auto& value : row->children) {
      if (value->kind == "Location" || value->kind == "Hint") {
        continue;
      }
      values.push_back(VisitExpression(*value));
    }
    rows.push_back(std::move(values));
  }
  return std::make_unique<InsertStatement>(Path(*path), std::move(rows),
                                           std::move(columns));
}

// Parses THEN RETURN projections ("*", column paths, or WITH ACTION *).
std::vector<NamedExpression> ParseReturning(
    const GoogleSqlAstNode& returning_clause) {
  std::vector<NamedExpression> projections;
  const GoogleSqlAstNode* list = returning_clause.Child("SelectList");
  if (list == nullptr) {
    for (const auto& child : returning_clause.children) {
      if (child->kind == "SelectList") {
        list = child.get();
        break;
      }
    }
  }
  if (list == nullptr) {
    return {};
  }
  for (const GoogleSqlAstNode* column : list->Children("SelectColumn")) {
    const GoogleSqlAstNode* expression_node = nullptr;
    for (const auto& child : column->children) {
      if (child->kind != "Alias") {
        expression_node = child.get();
        break;
      }
    }
    if (expression_node == nullptr) {
      throw std::runtime_error("GoogleSQL AST: empty RETURN item");
    }
    Expression expression = VisitExpression(*expression_node);
    std::string name = Alias(*column);
    if (name.empty() && expression->Type() == TypeTag::kColumnValue &&
        expression->AsColumnValue().GetColumnName().name != "*") {
      name = expression->AsColumnValue().GetColumnName().name;
    }
    projections.emplace_back(name, std::move(expression));
  }
  return projections;
}

std::unique_ptr<Statement> VisitUpdate(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  const GoogleSqlAstNode* items = root.Child("UpdateItemList");
  if (path == nullptr || items == nullptr) {
    throw std::runtime_error("GoogleSQL AST: bad UPDATE");
  }
  std::vector<std::pair<ColumnName, Expression>> assignments;
  for (const GoogleSqlAstNode* item : items->Children("UpdateItem")) {
    const GoogleSqlAstNode* set = item->Child("UpdateSetValue");
    if (set == nullptr || set->children.size() != 2) {
      // `SET (DELETE arr WHERE cond)`: nested array DML is not supported.
      throw std::runtime_error("GoogleSQL AST: bad UPDATE assignment");
    }
    Expression value = VisitExpression(*set->children[1]);
    ColumnName target(Path(*set->children[0]));
    if (set->children[0]->kind == "ArrayElement") {
      // arr[OFFSET(i)] = v  ==>  arr = __array_set(arr, i, v).
      const GoogleSqlAstNode& lhs = *set->children[0];
      const GoogleSqlAstNode* base_path = lhs.Child("PathExpression");
      const GoogleSqlAstNode* index_expr = nullptr;
      for (const auto& child : lhs.children) {
        if (child->kind == "FunctionCall" && !child->children.empty() &&
            child->children.front()->kind == "PathExpression") {
          for (size_t i = 1; i < child->children.size(); ++i) {
            if (child->children[i]->kind != "Location") {
              index_expr = child->children[i].get();
              break;
            }
          }
          break;
        }
        if (child->kind == "IntLiteral") {
          index_expr = child.get();
          break;
        }
      }
      if (base_path == nullptr || index_expr == nullptr) {
        throw std::runtime_error("GoogleSQL AST: bad array SET target");
      }
      target = ColumnName(Path(*base_path));
      value = FunctionCallExp(
          "__array_set", {ColumnValueExp(target), VisitExpression(*index_expr),
                          std::move(value)});
    }
    assignments.emplace_back(std::move(target), std::move(value));
  }
  // The WHERE clause is the single remaining child once the target path and
  // the assignment list are removed. Collect candidates instead of taking the
  // last one so unmapped siblings (hints etc.) cannot silently overwrite an
  // earlier WHERE.
  std::vector<const GoogleSqlAstNode*> where_candidates;
  const GoogleSqlAstNode* returning_clause = nullptr;
  std::string table_alias;
  for (const auto& child : root.children) {
    if (child->kind == "PathExpression" || child->kind == "UpdateItemList" ||
        child->kind == "Location" || child->kind == "Hint" ||
        child->kind == "Alias" || child->kind == "AssertRowsModified") {
      if (child->kind == "Alias") {
        if (const GoogleSqlAstNode* id = child->Child("Identifier")) {
          table_alias = Identifier(*id);
        }
      }
      continue;
    }
    if (child->kind == "ReturningClause") {
      returning_clause = child.get();
      continue;
    }
    where_candidates.push_back(child.get());
  }
  if (where_candidates.size() > 1) {
    throw std::runtime_error(
        "GoogleSQL AST: multiple UPDATE WHERE clause candidates");
  }
  Expression where;
  if (!where_candidates.empty()) {
    where = VisitExpression(*where_candidates.front());
  }
  auto statement = std::make_unique<UpdateStatement>(
      Path(*path), std::move(assignments), std::move(where));

  if (returning_clause != nullptr) {
    statement->SetReturning(ParseReturning(*returning_clause));
    if (std::getenv("TINYLAMB_DEBUG_GS")) {
      std::fprintf(stderr, "[vis] update returning attached\n");
    }
  } else if (std::getenv("TINYLAMB_DEBUG_GS")) {
    std::fprintf(stderr, "[vis] update NO returning clause\n");
  }
  return statement;
}

std::unique_ptr<Statement> VisitDelete(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    throw std::runtime_error("GoogleSQL AST: bad DELETE");
  }
  std::vector<const GoogleSqlAstNode*> where_candidates;
  const GoogleSqlAstNode* delete_returning = nullptr;
  for (const auto& child : root.children) {
    if (child->kind == "PathExpression" || child->kind == "Location" ||
        child->kind == "Hint") {
      continue;
    }
    if (child->kind == "ReturningClause") {
      delete_returning = child.get();
      continue;
    }
    where_candidates.push_back(child.get());
  }
  if (where_candidates.size() > 1) {
    throw std::runtime_error(
        "GoogleSQL AST: multiple DELETE WHERE clause candidates");
  }
  Expression where;
  if (!where_candidates.empty()) {
    where = VisitExpression(*where_candidates.front());
  }
  auto statement =
      std::make_unique<DeleteStatement>(Path(*path), std::move(where));
  if (delete_returning != nullptr) {
    statement->SetReturning(ParseReturning(*delete_returning));
  }
  return statement;
}

}  // namespace

std::unique_ptr<Statement> GoogleSqlAstVisitor::Visit(
    const GoogleSqlAstNode& root) {
  if (root.kind == "QueryStatement") {
    const GoogleSqlAstNode* query = root.Child("Query");
    if (query == nullptr) {
      throw std::runtime_error("GoogleSQL AST: missing query");
    }
    auto statement = VisitQuery(*query);
return std::make_unique<SelectStatement>(*statement);
  }
  if (root.kind == "CreateConstantStatement") {
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    if (path == nullptr) {
      throw std::runtime_error("GoogleSQL AST: bad CREATE CONSTANT");
    }
    std::string const_name = Path(*path);
    std::string const_val;
    for (const auto& child : root.children) {
      if (child->kind != "PathExpression" && child->kind != "Location") {
        Expression expr = VisitExpression(*child);
        Row dummy_row;
        Schema dummy_schema;
        try {
          Value v = expr->Evaluate(dummy_row, dummy_schema);
          if (v.type == ValueType::kVarChar) {
            const_val = std::string(v.value.varchar_value);
          } else {
            const_val = v.AsString();
          }
        } catch (...) {
          if (child->kind == "StringLiteral") {
            const_val = DecodeString(*child);
          }
        }
        break;
      }
    }
    SetSessionConstant(const_name, const_val);
    std::vector<NamedExpression> proj;
    proj.emplace_back("constant",
                      ConstantValueExp(Value(std::string(const_name))));
    return std::make_unique<SelectStatement>(
        std::move(proj), std::vector<std::string>{}, Expression{});
  }
  if (root.kind == "CreateTableStatement") {
    return VisitCreate(root);
  }
  if (root.kind == "InsertStatement") {
    return VisitInsert(root);
  }
  if (root.kind == "UpdateStatement") {
    return VisitUpdate(root);
  }
  if (root.kind == "DeleteStatement") {
    return VisitDelete(root);
  }
  if (root.kind == "DropStatement" || root.kind == "DropStatement TABLE") {
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    if (path == nullptr) {
      throw std::runtime_error("GoogleSQL AST: bad DROP");
    }
    return std::make_unique<DropTableStatement>(Path(*path));
  }
  if (root.kind.starts_with("DropStatement")) {
    throw std::runtime_error("GoogleSQL AST: unsupported statement " +
                             root.kind);
  }
  throw std::runtime_error("GoogleSQL AST: unsupported statement " + root.kind);
}

}  // namespace tinylamb
