/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/googlesql_ast_visitor.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "database/transaction_context.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "executor/detail/expression_eval.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
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

int ParseTimeZoneOffset(std::string_view tz_str, int Y, int M, int D, int h, int m, int s, int default_offset = 0) {
  if (tz_str.empty()) { return default_offset; }
  if (tz_str == "UTC" || tz_str == "GMT" || tz_str == "utc" || tz_str == "gmt" ||
      tz_str == "Z" || tz_str == "z" || tz_str == "Etc/Greenwich" || tz_str == "Etc/UTC" || tz_str == "Etc/GMT") {
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
  if (zone_name == "NZ-CHAT") { zone_name = "Pacific/Chatham"; }
  try {
    const auto* zone = std::chrono::locate_zone(zone_name);
    if (zone) {
      int y = Y < 1970 ? 1970 : Y;
      std::chrono::year_month_day ymd{std::chrono::year{y},
                                      std::chrono::month{static_cast<unsigned>(M)},
                                      std::chrono::day{static_cast<unsigned>(D)}};
      std::chrono::local_days loc_d{ymd};
      auto loc_tp = loc_d + std::chrono::hours{h} +
                    std::chrono::minutes{m} + std::chrono::seconds{s};
      auto loc_info = zone->get_info(loc_tp);
      return static_cast<int>(loc_info.first.offset.count());
    }
  } catch (...) {}
  return default_offset;
}

std::string SqlTypeFromAst(const GoogleSqlAstNode& node);
std::string InferSubqueryArrayElementType(const GoogleSqlAstNode& query_node);
std::string InferAggregateArrayElementType(const GoogleSqlAstNode& node);

std::string Lower(std::string value) {

  std::ranges::transform(
      value, value.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
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
      !text.empty() && std::ranges::all_of(text, [](char c) {
        return '0' <= c && c <= '9';
      });
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
      !text.empty() && std::ranges::all_of(text, [](char c) {
        return '0' <= c && c <= '9';
      });
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
    value = DecodeSingleComponent("\"" + value.substr(1, value.size() - 2) + "\"");
  }
  return value;
}

std::vector<std::string> PathParts(const GoogleSqlAstNode& path) {
  std::vector<std::string> result;
  for (const auto& child : path.children) {
    if (child->kind == "Identifier") { result.push_back(Identifier(*child));
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
    if (!result.empty()) { result += '.';
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
  if (!is_raw && value.size() >= 1 && (value.front() == 'r' || value.front() == 'R')) {
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
      if (next == 'a') { decoded.push_back('\a'); }
      else if (next == 'b') { decoded.push_back('\b'); }
      else if (next == 'f') { decoded.push_back('\f'); }
      else if (next == 'n') { decoded.push_back('\n'); }
      else if (next == 'r') { decoded.push_back('\r'); }
      else if (next == 't') { decoded.push_back('\t'); }
      else if (next == 'v') { decoded.push_back('\v'); }
      else if (next == '\\') { decoded.push_back('\\'); }
      else if (next == '\'') { decoded.push_back('\''); }
      else if (next == '"') { decoded.push_back('"'); }
      else if (next == '`') { decoded.push_back('`'); }
      else if (next == '?' || next == '/') { decoded.push_back(next); }
      else if (next >= '0' && next <= '7') {
        std::string oct_str;
        oct_str.push_back(next);
        if (i + 1 < value.size() && value[i + 1] >= '0' && value[i + 1] <= '7') {
          oct_str.push_back(value[++i]);
          if (i + 1 < value.size() && value[i + 1] >= '0' && value[i + 1] <= '7') {
            oct_str.push_back(value[++i]);
          }
        }
        try {
          uint32_t raw_byte = std::stoul(oct_str, nullptr, 8);
          if (is_bytes || raw_byte <= 0x7F) {
            decoded.push_back(static_cast<char>(raw_byte));
          } else {
            decoded.push_back(static_cast<char>(0xC0 | ((raw_byte >> 6) & 0x1F)));
            decoded.push_back(static_cast<char>(0x80 | (raw_byte & 0x3F)));
          }
        } catch (...) {
          decoded.push_back(next);
        }
      }
      else if (next == 'x' || next == 'X') {
        if (i + 2 < value.size()) {
          std::string hex_str = value.substr(i + 1, 2);
          try {
            uint32_t raw_byte = std::stoul(hex_str, nullptr, 16);
            if (is_bytes || raw_byte <= 0x7F) {
              decoded.push_back(static_cast<char>(raw_byte));
            } else {
              decoded.push_back(static_cast<char>(0xC0 | ((raw_byte >> 6) & 0x1F)));
              decoded.push_back(static_cast<char>(0x80 | (raw_byte & 0x3F)));
            }
            i += 2;
          } catch (...) {
            decoded.push_back(next);
          }
        } else {
          decoded.push_back(next);
        }
      }
      else if (next == 'u') {
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
    } else if (!is_triple && value[i] == quote && i + 1 < value.size() && value[i + 1] == quote) {
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



// Recognizes the correlated shape `SELECT x FROM UNNEST(<expr>) [AS x]`
// (no other clauses) used by quantified comparisons over table arrays and
// returns the array expression node.  Returns nullptr for anything else.
const GoogleSqlAstNode* UnnestArrayOfQuantifiedSubquery(  // NOLINT(misc-no-recursion)
    const GoogleSqlAstNode& query) {
  const GoogleSqlAstNode* select = (query.kind == "Select")
                                       ? &query
                                       : query.Child("Select");
  if (select == nullptr) { return nullptr; }
  for (const auto& child : select->children) {
    if (child->kind == "Location") { continue; }
    if (child->kind != "SelectList" && child->kind != "FromClause") {
      return nullptr;
    }
  }
  const GoogleSqlAstNode* select_list = select->Child("SelectList");
  if (select_list == nullptr || select_list->children.size() != 1) {
    return nullptr;
  }
  const GoogleSqlAstNode* from = select->Child("FromClause");
  if (from == nullptr || from->children.size() != 1) { return nullptr; }
  const GoogleSqlAstNode* source = from->children.front().get();
  if (source == nullptr || source->kind != "TablePathExpression") {
    return nullptr;
  }
  const GoogleSqlAstNode* unnest = source->Child("UnnestExpression");
  if (unnest == nullptr) { return nullptr; }
  if (source->Child("WithOffset") != nullptr ||
      source->Child("WithOffsetClause") != nullptr) {
    return nullptr;
  }
  const GoogleSqlAstNode* expr_with_alias =
      unnest->Child("ExpressionWithOptAlias");
  if (expr_with_alias == nullptr) { return nullptr; }
  for (const auto& child : expr_with_alias->children) {
    if (child->kind != "Location" && child->kind != "Identifier" &&
        child->kind != "Alias") {
      return child.get();
    }
  }
  return nullptr;
}

BinaryOperation BinaryOp(std::string_view detail) {
  if (detail == "+") { return BinaryOperation::kAdd;
}
  if (detail == "-") { return BinaryOperation::kSubtract;
}
  if (detail == "*") { return BinaryOperation::kMultiply;
}
  if (detail == "/") { return BinaryOperation::kDivide;
}
  if (detail == "%") { return BinaryOperation::kModulo;
}
  if (detail == "=") { return BinaryOperation::kEquals;
}
  if (detail == "!=" || detail == "<>") { return BinaryOperation::kNotEquals;
}
  if (detail == "<") { return BinaryOperation::kLessThan;
}
  if (detail == "<=") { return BinaryOperation::kLessThanEquals;
}
  if (detail == ">") { return BinaryOperation::kGreaterThan;
}
  if (detail == ">=") { return BinaryOperation::kGreaterThanEquals;
}
  if (detail == "LIKE") { return BinaryOperation::kLike;
}
  if (detail == "NOT LIKE") { return BinaryOperation::kNotLike;
}
  throw std::runtime_error("GoogleSQL AST: unsupported binary operator " +
                           std::string(detail));
}

std::shared_ptr<SelectStatement> VisitQuery(const GoogleSqlAstNode& query);
Expression VisitExpression(const GoogleSqlAstNode& node);

bool NeedsRelationalEvaluation(const Expression& expression,  // NOLINT(misc-no-recursion) // AST traversal recursion is intentional; expression depth bounded by ExpressionDepthGuard (kMaxExpressionDepth).
                               bool top_level = true) {
  if (!expression) { return false;
}
  switch (expression->Type()) {
    case TypeTag::kQueryExp:
    case TypeTag::kIntervalExp:
      return true;
    case TypeTag::kAggregateExp: {
      const AggregateExpression& aggregate =
          expression->AsAggregateExpression();
      // Statistical / sketching aggregates are only implemented by the
      // relational interpreter's accumulator; keep them off the physical
      // aggregation operators at any nesting depth.
      if (IsExtendedAggregate(aggregate.GetType())) {
        return true;
      }
      return !top_level || NeedsRelationalEvaluation(aggregate.Child());
    }
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
      if (NeedsRelationalEvaluation(value.child_, false)) { return true;
}
      return std::ranges::any_of(value.list_, [](const Expression& item) {  // NOLINT(misc-no-recursion) // Part of NeedsRelationalEvaluation recursion; depth-guarded by ExpressionDepthGuard.
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
      return std::ranges::any_of(
          expression->AsArrayExpression().Elements(),
          [](const Expression& element) {
            return NeedsRelationalEvaluation(element, false);
          });
    default:
      return false;
  }
}

Expression FoldBoolean(const GoogleSqlAstNode& node, BinaryOperation op) {  // NOLINT(misc-no-recursion) // AST traversal recursion is intentional; depth bounded by ExpressionDepthGuard in VisitExpression.
  Expression result;
  for (const auto& child : node.children) {
    Expression next = VisitExpression(*child);
    result = result
                 ? BinaryExpressionExp(std::move(result), op, std::move(next))
                 : std::move(next);
  }
  if (!result) { throw std::runtime_error("GoogleSQL AST: empty boolean node");
}
  return result;
}

bool IsBooleanAstNode(const GoogleSqlAstNode& node) {
  if (node.kind == "BooleanLiteral") { return true; }
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
  if (node.kind == "BytesLiteral") { return true; }
  if (node.kind == "CastExpression" && node.children.size() >= 2) {
    const GoogleSqlAstNode& type_node = *node.children[1];
    std::string type_name = SqlTypeFromAst(type_node);
    if (type_name.empty()) {
      if (const auto* path = type_node.Child("PathExpression")) {
        type_name = Path(*path);
      } else if (type_node.kind == "SimpleType") {
        for (const auto& c : type_node.children) {
          type_name = SqlTypeFromAst(*c);
          if (!type_name.empty()) { break; }
        }
      }
    }
    if (UpperCopy(type_name).find("BYTES") != std::string::npos ||
        type_node.detail.find("BYTES") != std::string::npos) {
      return true;
    }
  }
  if (node.kind == "FunctionCall") {
    if (!node.children.empty() && node.children.front()->kind == "PathExpression") {
      const std::string fn = Lower(Path(*node.children.front()));
      if (fn == "byte_substr" || fn == "b") { return true; }
    }
  }
  return false;
}


WindowOrderTerm ParseOrderingTerm(const GoogleSqlAstNode* term) {
  WindowOrderTerm parsed;
  if (term == nullptr || term->children.empty()) { return parsed; }
  for (const auto& child : term->children) {
    if (child->kind == "Location") { continue; }
    if (child->kind == "NullOrder") {
      parsed.nulls_first =
          UpperCopy(child->detail).find("NULLS FIRST") != std::string::npos;
      continue;
    }
    if (!parsed.expression) { parsed.expression = VisitExpression(*child); }
  }
  parsed.ascending = UpperCopy(term->detail) != "DESC";
  return parsed;
}

std::vector<WindowOrderTerm> ParseOrderingList(const GoogleSqlAstNode& order) {
  std::vector<WindowOrderTerm> terms;
  for (const GoogleSqlAstNode* term : order.Children("OrderingExpression")) {
    WindowOrderTerm parsed = ParseOrderingTerm(term);
    if (parsed.expression) { terms.push_back(std::move(parsed)); }
  }
  return terms;
}

Expression VisitFunction(const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion) // AST traversal recursion is intentional; depth bounded by ExpressionDepthGuard in VisitExpression.
  if (node.children.empty() ||
      node.children.front()->kind != "PathExpression") {
    throw std::runtime_error("GoogleSQL AST: function without name");
  }
  std::string name = Lower(Path(*node.children.front()));
  if (name == "ucase") { name = "upper"; }
  if (name == "lcase") { name = "lower"; }
  if (name == "collate") {
    // GoogleSQL requires COLLATE(value, 'literal'): the collator must be a
    // plain string literal, not a NULL/parameter/expression.
    std::vector<const GoogleSqlAstNode*> args;
    for (size_t i = 1; i < node.children.size(); ++i) {
      if (node.children[i]->kind != "Location") {
        args.push_back(node.children[i].get());
      }
    }
    if (args.size() != 2 || args[1]->kind != "StringLiteral") {
      throw std::runtime_error(
          "The second argument of COLLATE() must be a string literal");
    }
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
    if (child.kind == "Location") { continue; }
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
    arguments.push_back(std::move(arg));
  }

  auto finish_aggregate = [&](AggregationType type) -> Expression {
    auto aggregate = std::make_shared<AggregateExpression>(
        type, arguments.empty() ? nullptr : arguments[0],
        node.detail.find("distinct=true") != std::string::npos);
    if (having != AggregateHavingModifier::kNone) {
      aggregate->SetHaving(having, having_condition);
    }
    if (where_filter) { aggregate->SetWhereFilter(where_filter); }
    if (!inner_order_by.empty()) { aggregate->SetInnerOrderBy(inner_order_by); }
    if (inner_limit.has_value()) { aggregate->SetInnerLimit(inner_limit); }
    if (type == AggregationType::kStringAgg && arguments.size() > 1) {
      aggregate->SetSecondaryArg(arguments[1]);
    } else if (IsExtendedAggregate(type) && arguments.size() > 1) {
      aggregate->SetTrailingArgs({arguments.begin() + 1, arguments.end()});
    }
    // APPROX_TOP_COUNT(value, number) / APPROX_TOP_SUM(value, weight,
    // number): the trailing arguments ride along as per-row extras.
    if (type == AggregationType::kApproxTopCount ||
        type == AggregationType::kApproxTopSum) {
      std::vector<Expression> extras;
      for (size_t i = 1; i < arguments.size(); ++i) {
        extras.push_back(arguments[i]);
      }
      aggregate->SetExtraArgs(std::move(extras));
    }
    // ARRAY_AGG keeps a statically inferred element type: runtime values
    // cannot distinguish BOOL from INT64 or INT32 from INT64.
    if (type == AggregationType::kArrayAgg) {
      for (size_t i = 1; i < node.children.size(); ++i) {
        const GoogleSqlAstNode& child = *node.children[i];
        if (child.kind == "Location" || child.kind == "WhereClause" ||
            child.kind == "HavingModifier" || child.kind == "OrderBy" ||
            child.kind == "LimitOffset") {
          continue;
        }
        const std::string element_type =
            InferAggregateArrayElementType(child);
        if (!element_type.empty()) {
          aggregate->SetArrayElementSqlType(element_type);
        }
        break;
      }
    }
    return aggregate;
  };

  if (name == "count" || name == "sum" || name == "avg" || name == "min" ||
      name == "max" || name == "logical_and" || name == "logical_or" ||
      name == "array_agg" || name == "string_agg" || name == "countif" ||
      name == "bit_and" || name == "bit_or" || name == "bit_xor" ||
      name == "array_concat_agg" || name == "elementwise_sum" ||
      name == "elementwise_avg" || name == "any_value" ||
      name == "approx_top_count" || name == "approx_top_sum") {
    const bool is_bit = name == "bit_and" || name == "bit_or" ||
                        name == "bit_xor";
    const bool is_approx_top =
        name == "approx_top_count" || name == "approx_top_sum";
    const size_t approx_arity = name == "approx_top_count" ? 2 : 3;
    // STRING_AGG accepts 1 or 2 arguments and is exempt from the strict
    // single-argument aggregate arity check.
    const bool arity_checked =
        !is_bit && !is_approx_top && name != "string_agg";
    const bool arity_ok =
        !arity_checked ||
        (name == "count" && !arguments.empty()) ||
        arguments.size() == 1 ||
        (is_bit && arguments.size() == 2) ||
        (is_approx_top && arguments.size() == approx_arity);
    if (!arity_ok) {
      throw std::runtime_error("GoogleSQL AST: aggregate arity");
    }
    AggregationType type = AggregationType::kCount;
    if (name == "sum") { type = AggregationType::kSum; }
    if (name == "avg") { type = AggregationType::kAvg; }
    if (name == "min") { type = AggregationType::kMin; }
    if (name == "max") { type = AggregationType::kMax; }
    if (name == "logical_and") { type = AggregationType::kLogicalAnd; }
    if (name == "logical_or") { type = AggregationType::kLogicalOr; }
    if (name == "array_agg") { type = AggregationType::kArrayAgg; }
    if (name == "string_agg") { type = AggregationType::kStringAgg; }
    if (name == "countif") { type = AggregationType::kCountIf; }
    // ANY_VALUE may legally return any non-NULL group value; MIN provides
    // that with deterministic streaming semantics.
    if (name == "any_value") { type = AggregationType::kMin; }
    if (name == "bit_and") { type = AggregationType::kBitAnd; }
    if (name == "bit_or") { type = AggregationType::kBitOr; }
    if (name == "bit_xor") { type = AggregationType::kBitXor; }
    if (name == "array_concat_agg") { type = AggregationType::kArrayConcatAgg; }
    if (name == "elementwise_sum") { type = AggregationType::kElementwiseSum; }
    if (name == "elementwise_avg") { type = AggregationType::kElementwiseAvg; }
    if (name == "approx_top_count") { type = AggregationType::kApproxTopCount; }
    if (name == "approx_top_sum") { type = AggregationType::kApproxTopSum; }
    return finish_aggregate(type);
  }

  // Statistical and approximate aggregates. Multi-argument forms keep their
  // first argument as the aggregate child; the rest ride along as trailing
  // arguments evaluated per row.
  {
    AggregationType extended{};
    size_t min_arity = 1;
    size_t max_arity = 1;
    bool matched = true;
    if (name == "any_value") {
      extended = AggregationType::kAnyValue;
    } else if (name == "var_samp" || name == "variance") {
      extended = AggregationType::kVarSamp;
    } else if (name == "var_pop") {
      extended = AggregationType::kVarPop;
    } else if (name == "stddev_samp" || name == "stddev") {
      extended = AggregationType::kStddevSamp;
    } else if (name == "stddev_pop") {
      extended = AggregationType::kStddevPop;
    } else if (name == "covar_samp") {
      extended = AggregationType::kCovarSamp;
      min_arity = max_arity = 2;
    } else if (name == "covar_pop") {
      extended = AggregationType::kCovarPop;
      min_arity = max_arity = 2;
    } else if (name == "corr") {
      extended = AggregationType::kCorr;
      min_arity = max_arity = 2;
    } else if (name == "approx_quantiles") {
      extended = AggregationType::kApproxQuantiles;
      min_arity = max_arity = 2;
    } else if (name == "approx_top_count") {
      extended = AggregationType::kApproxTopCount;
      min_arity = max_arity = 2;
    } else if (name == "approx_top_sum") {
      extended = AggregationType::kApproxTopSum;
      min_arity = max_arity = 3;
    } else if (name == "hll_count.init") {
      extended = AggregationType::kHllInit;
      max_arity = 2;
    } else if (name == "hll_count.merge") {
      extended = AggregationType::kHllMerge;
    } else if (name == "hll_count.merge_partial") {
      extended = AggregationType::kHllMergePartial;
    } else if (name == "kll_quantiles.init_int64") {
      extended = AggregationType::kKllInitInt64;
      max_arity = 2;
    } else if (name == "kll_quantiles.init_uint64") {
      extended = AggregationType::kKllInitUint64;
      max_arity = 2;
    } else if (name == "kll_quantiles.init_double") {
      extended = AggregationType::kKllInitDouble;
      max_arity = 2;
    } else if (name == "kll_quantiles.merge_partial") {
      extended = AggregationType::kKllMergePartial;
    } else {
      matched = false;
    }
    if (matched) {
      if (arguments.size() < min_arity || arguments.size() > max_arity) {
        throw std::runtime_error("GoogleSQL AST: aggregate arity");
      }
      return finish_aggregate(extended);
    }
  }
  return FunctionCallExp(name, std::move(arguments));
}





bool IsAstTrivia(std::string_view kind) {
  return kind == "Location";
}

bool IsArrayTypeNode(std::string_view kind) {
  return kind == "ArrayType";
}

std::string SqlTypeFromAst(const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion)
  if (IsAstTrivia(node.kind)) { return {}; }
  if (node.kind == "PathExpression") { return UpperCopy(Path(node)); }
  if (node.kind == "Identifier") { return UpperCopy(node.detail); }
  if (node.kind == "SimpleType") {
    // Preserve length/type parameters (STRING(2), NUMERIC(10), ...): they
    // carry validation semantics for casts.
    std::string base;
    for (const auto& child : node.children) {
      if (child->kind == "PathExpression" || child->kind == "Identifier") {
        base = child->kind == "Identifier" ? UpperCopy(child->detail)
                                           : UpperCopy(Path(*child));
        break;
      }
    }
    if (!base.empty()) {
      for (const auto& child : node.children) {
        if (child->kind != "TypeParameterList") { continue; }
        std::string params;
        for (const auto& param : child->children) {
          if (param->kind == "IntLiteral" || param->kind == "Identifier") {
            if (!params.empty()) { params += ", "; }
            params += param->kind == "IntLiteral" ? param->detail
                                                  : UpperCopy(param->detail);
          }
        }
        if (!params.empty()) { base += "(" + params + ")"; }
        break;
      }
    }
    if (!base.empty()) { return base; }
  }
  if (node.kind == "ArrayType") {
    for (const auto& child : node.children) {
      const std::string nested = SqlTypeFromAst(*child);
      if (!nested.empty()) { return "ARRAY<" + nested + ">"; }
    }
    return "ARRAY<INT64>";
  }
  for (const auto& child : node.children) {
    const std::string nested = SqlTypeFromAst(*child);
    if (!nested.empty()) { return nested; }
  }
  return {};
}

std::string InferArrayElementSqlType(const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion)
  if (node.kind == "BooleanLiteral") { return "BOOL"; }
  if (node.kind == "FloatLiteral") { return "FLOAT64"; }
  if (node.kind == "StringLiteral") { return "STRING"; }
  if (node.kind == "BytesLiteral") { return "BYTES"; }
  if (node.kind == "DateOrTimeLiteral") {
    if (node.detail == "TYPE_DATE") { return "DATE"; }
    if (node.detail == "TYPE_TIMESTAMP") { return "TIMESTAMP"; }
    if (node.detail == "TYPE_TIME") { return "TIME"; }
    if (node.detail == "TYPE_DATETIME") { return "DATETIME"; }
    return "STRING";
  }
  if (node.kind == "IntervalExpr") { return "INTERVAL"; }
  if (node.kind == "CastExpression" && node.children.size() >= 2) {
    return SqlTypeFromAst(*node.children[1]);
  }
  if (node.kind == "NewConstructor") { return "PROTO"; }
  if (node.kind == "NullLiteral") { return {}; }


  if (node.kind == "ArrayConstructor") {
    std::string inner;
    for (const auto& child : node.children) {
      if (IsAstTrivia(child->kind)) { continue; }
      if (child->kind == "ArrayType") {
        inner = SqlTypeFromAst(*child);
        if (inner.starts_with("ARRAY<") && inner.back() == '>') {
          inner = inner.substr(6, inner.size() - 7);
        }
        break;
      }
      inner = InferArrayElementSqlType(*child);
      if (!inner.empty()) { break; }
    }
    if (inner.empty()) { inner = "INT64"; }
    return "ARRAY<" + inner + ">";
  }
  return "INT64";
}

// Statically infers the SQL type of an ARRAY(SELECT ...) subquery's single
// projected column from its AST.  Returns "" when the projection is not a
// statically-known literal/cast shape; callers then fall back to runtime
// inference from the produced values.
std::string InferSubqueryArrayElementType(
    const GoogleSqlAstNode& query_node) {
  const GoogleSqlAstNode* select = query_node.Child("Select");
  const GoogleSqlAstNode* select_list =
      select == nullptr ? nullptr : select->Child("SelectList");
  if (select_list == nullptr) { return {};
}
  for (const GoogleSqlAstNode* column : select_list->Children("SelectColumn")) {
    const GoogleSqlAstNode* expression_node = nullptr;
    for (const auto& child : column->children) {
      if (child->kind != "Alias") {
        expression_node = child.get();
        break;
      }
    }
    if (expression_node == nullptr) { return {};
}
    const GoogleSqlAstNode& expr = *expression_node;
    if (expr.kind == "BooleanLiteral") { return "BOOL"; }
    if (expr.kind == "FloatLiteral") { return "DOUBLE"; }
    if (expr.kind == "IntLiteral") { return "INT64"; }
    if (expr.kind == "StringLiteral") { return "STRING"; }
    if (expr.kind == "BytesLiteral") { return "BYTES"; }
    if (expr.kind == "CastExpression" && expr.children.size() >= 2) {
      return SqlTypeFromAst(*expr.children[1]);
    }
    return "";
  }
  return "";
}

// Reliable-only variant used by ARRAY_AGG: unlike the subquery path there is
// no fallback cost asymmetry — a wrong guess (e.g. INT64 for a DOUBLE column)
// is worse than deferring to runtime value inference, so only literal/cast
// argument shapes produce a type here.
std::string InferAggregateArrayElementType(const GoogleSqlAstNode& node) {
  if (node.kind == "BooleanLiteral") { return "BOOL"; }
  if (node.kind == "StringLiteral") { return "STRING"; }
  if (node.kind == "BytesLiteral") { return "BYTES"; }
  if (node.kind == "DateOrTimeLiteral") {
    if (node.detail == "TYPE_DATE") { return "DATE"; }
    if (node.detail == "TYPE_TIMESTAMP") { return "TIMESTAMP"; }
    if (node.detail == "TYPE_TIME") { return "TIME"; }
    if (node.detail == "TYPE_DATETIME") { return "DATETIME"; }
    return {};
  }
  if (node.kind == "CastExpression" && node.children.size() >= 2) {
    return SqlTypeFromAst(*node.children[1]);
  }
  return "";
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
};

thread_local std::unordered_map<std::string, NamedWindowParts>
    t_named_windows;

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
    parts.frame_unit = UpperCopy(frame->detail) == "RANGE"
                           ? WindowFrameUnit::kRange
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
      parts.has_frame = true;
      parts.frame_start = ParseFrameBound(*bounds[0]);
      parts.frame_end = parts.frame_start;
    } else {
      throw std::runtime_error(
          "GoogleSQL AST: window frame requires BETWEEN x AND y");
    }
  }
  return parts;
}

Expression VisitAnalyticFunctionCall(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* call = node.Child("FunctionCall");
  if (call == nullptr || call->children.empty()) {
    throw std::runtime_error(
        "GoogleSQL AST: malformed analytic function call");
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
    if (child.kind == "Location") { continue; }
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
          window->inner_limit = static_cast<size_t>(ParseUnsignedLiteral(*value));
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
        spec->Child("OrderBy") == nullptr && spec->Child("WindowFrame") == nullptr) {
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
    } else {
      NamedWindowParts parts = ParseWindowSpecification(*spec);
      window->partition_by = std::move(parts.partition_by);
      window->order_by = std::move(parts.order_by);
      window->frame_unit = parts.frame_unit;
      window->frame_start = parts.frame_start;
      window->frame_end = parts.frame_end;
      window->has_frame = parts.has_frame;
    }
  }
  return window;
}


// Normalizes a TIMESTAMP string to UTC ("...+00"), interpreting an explicit
// offset / UTC marker when present and the session default time zone
// otherwise.  Shared by TIMESTAMP literals and typed array elements.
std::string NormalizeTimestampText(const std::string& text) {
  std::string norm_ts = text;
  if (text.size() < 10) { return norm_ts; }
  size_t tz_pos = std::string::npos;
  for (size_t i = 10; i < text.size(); ++i) {
    if (text[i] == '+' || text[i] == '-') {
      tz_pos = i;
      break;
    }
  }
  int total_offset_mins = 0;
  bool has_explicit_tz = false;
  if (text.find("UTC") != std::string::npos || text.find("utc") != std::string::npos ||
      text.find('Z') != std::string::npos || text.find('z') != std::string::npos) {
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
      } catch (...) {}
    } else {
      try { tz_hours = std::stoi(tz_part); } catch (...) {}
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
  double s_val = 0;
  int matched = sscanf(base_time.c_str(), "%d-%d-%d %d:%d:%lf", &Y, &M, &D, &h, &m, &s_val);
  if (matched < 3) {
    matched = sscanf(base_time.c_str(), "%d-%d-%d", &Y, &M, &D);
  }
  if (matched < 3) { return norm_ts; }
  if (is_leap_sec) { m += 1; s_val = 0.0; }
  if (!has_explicit_tz) {
    total_offset_mins =
        ParseTimeZoneOffset(GetDefaultTimeZone(), Y, M, D, h, m,
                            static_cast<int>(s_val), -8 * 3600) / 60;
  }
  struct tm t = {};
  t.tm_year = Y - 1900;
  t.tm_mon = M - 1;
  t.tm_mday = D;
  t.tm_hour = h;
  t.tm_min = m - total_offset_mins;
  t.tm_sec = static_cast<int>(s_val);
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
             utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday,
             utc.tm_hour, utc.tm_min, utc.tm_sec, frac_str.c_str());
  } else {
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d+00",
             utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday,
             utc.tm_hour, utc.tm_min, utc.tm_sec);
  }
  return std::string(buf);
}

Expression VisitExpression(const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion) // Recursive AST descent by design; stack overflow guarded via ExpressionDepthGuard.
  const ExpressionDepthGuard depth_guard;
  if (node.kind == "PathExpression") {
    std::string path_name = Path(node);
    if (HasSessionConstant(path_name)) {
      return ConstantValueExp(Value(GetSessionConstant(path_name)));
    }
    return ColumnValueExp(path_name);
  }
  if (node.kind == "Star") { return ColumnValueExp("*");
}
  if (node.kind == "IntLiteral") {
    return ConstantValueExp(Value(ParseIntLiteral(node)));
  }
  if (node.kind == "FloatLiteral") {
    return ConstantValueExp(Value(ParseFloatLiteral(node)));
  }
  if (node.kind == "StringLiteral") {
    return ConstantValueExp(Value(DecodeString(node)));
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
          std::chrono::year_month_day ymd{std::chrono::year{Y},
                                          std::chrono::month{static_cast<unsigned>(M)},
                                          std::chrono::day{static_cast<unsigned>(D)}};
          int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count() + h / 24;
          h %= 24;
          std::chrono::sys_days new_sd{std::chrono::days{days}};
          std::chrono::year_month_day new_ymd{new_sd};
          char buf[64];
          snprintf(buf, sizeof(buf), "%04d-%02u-%02u %02d:00:00",
                   int(new_ymd.year()), unsigned(new_ymd.month()), unsigned(new_ymd.day()), h);
          dt_str = buf;
        }
      }
      return ConstantValueExp(Value(std::move(dt_str)));
    }
    if (node.detail == "TYPE_TIME") {
      return ConstantValueExp(Value(std::string(text)));
    }
    // TYPE_TIMESTAMP
    return ConstantValueExp(Value(NormalizeTimestampText(text)));
  }


  if (node.kind == "NullLiteral") { return ConstantValueExp(Value());
}
  // INSERT ... VALUES (30, DEFAULT): tables built by this engine carry no
  // column defaults, so DEFAULT resolves to NULL.
  if (node.kind == "DefaultLiteral") { return ConstantValueExp(Value());
}
  if (node.kind == "BooleanLiteral") {
    const std::string upper_literal = UpperCopy(node.detail);
    return ConstantValueExp(Value(upper_literal == "TRUE"));
  }
  if (node.kind == "ArrayConstructor") {
    std::string element_type;
    std::vector<Expression> elements;
    for (const auto& child : node.children) {
      if (IsAstTrivia(child->kind)) { continue; }
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
      // Typed TIMESTAMP arrays normalize string elements to UTC, mirroring
      // standalone TIMESTAMP literal handling.
      if (UpperCopy(element_type) == "TIMESTAMP" &&
          child->kind == "StringLiteral") {
        elements.push_back(ConstantValueExp(
            Value(NormalizeTimestampText(DecodeString(*child)))));
        continue;
      }
      elements.push_back(VisitExpression(*child));
    }
    if (element_type.empty()) {
      for (const auto& child : node.children) {
        if (IsAstTrivia(child->kind) || IsArrayTypeNode(child->kind)) {
          continue;
        }
        // ARRAY(SELECT ...): infer from the subquery's projected column.
        if (child->kind == "ExpressionSubquery" &&
            child->detail == "modifier=ARRAY") {
          if (const GoogleSqlAstNode* inner_query = child->Child("Query")) {
            element_type = InferSubqueryArrayElementType(*inner_query);
            if (!element_type.empty()) { break; }
          }
        }
        element_type = InferArrayElementSqlType(*child);
        if (!element_type.empty()) { break; }
      }
    }
    if (element_type.empty()) { element_type = "INT64"; }
    // ARRAY(SELECT ...) projects the whole subquery result as one array
    // value; a plain QueryExpression would only expose its first row.
    if (elements.size() == 1 && elements[0]->Type() == TypeTag::kQueryExp) {
      const QueryExpression& query = elements[0]->AsQueryExpression();
      if (!query.Exists() && !query.Test()) {
        auto array_query = std::make_shared<QueryExpression>(
            query.Query(), nullptr, false, false);
        array_query->SetArrayResult(true);
        array_query->SetArrayElementSqlType(element_type);
        return Expression(array_query);
      }    }
    return ArrayExpressionExp(std::move(elements), std::move(element_type));
  }
  if (node.kind == "BinaryExpression") {
    if (node.children.size() != 2) {
      throw std::runtime_error("GoogleSQL AST: binary expression arity");
    }
    Expression left = VisitExpression(*node.children[0]);
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
        const bool val = (rhs_detail == "TRUE" || rhs_detail == "true");
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
    return BinaryExpressionExp(std::move(left), BinaryOp(node.detail),
                               std::move(right));
  }

  if (node.kind == "AndExpr") { return FoldBoolean(node, BinaryOperation::kAnd); }
  if (node.kind == "OrExpr") { return FoldBoolean(node, BinaryOperation::kOr); }
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
    bool all_arrays = true;
    for (const auto& child : node.children) {
      if (child->kind == "Location") { continue; }
      Expression arg = VisitExpression(*child);
      if (arg->Type() != TypeTag::kArrayExp) {
        all_arrays = false;
      }
      arg = WrapIfBoolean(std::move(arg), *child);
      args.push_back(std::move(arg));
    }
    // array || array is array concatenation, not string concat.
    if (all_arrays && args.size() >= 2) {
      return FunctionCallExp("array_concat", std::move(args));
    }
    return FunctionCallExp("concat", std::move(args));
  }

  if (node.kind == "FunctionCall") { return VisitFunction(node); }

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
    auto accessor_name = [](const GoogleSqlAstNode& call) -> std::string {
      if (call.children.empty() ||
          call.children.front()->kind != "PathExpression") {
        return {};
      }
      std::string name = UpperCopy(Path(*call.children.front()));
      if (name == "OFFSET" || name == "ORDINAL" || name == "SAFE_OFFSET" ||
          name == "SAFE_ORDINAL") {
        return name;
      }
      return {};
    };
    for (const auto& child : node.children) {
      if (child->kind == "Location") { continue; }
      if (child->kind == "FunctionCall") {
        const std::string name = accessor_name(*child);
        if (!name.empty()) {
          accessor = name;
          for (size_t i = 1; i < child->children.size(); ++i) {
            if (child->children[i]->kind != "Location") {
              index_node = child->children[i].get();
              break;
            }
          }
          continue;
        }
      }
      if (!base && child->kind != "Location") { base = VisitExpression(*child); }
    }
    if (!base || index_node == nullptr) {
      throw std::runtime_error("GoogleSQL AST: malformed array element");
    }
    std::string fn = "array_element_offset";
    if (accessor == "SAFE_OFFSET") {
      fn = "array_element_safe_offset";
    } else if (accessor == "SAFE_ORDINAL") {
      fn = "array_element_safe_ordinal";
    } else if (accessor == "ORDINAL") {
      fn = "array_element_ordinal";
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
      Expression cond = BinaryExpressionExp(value_expr, BinaryOperation::kEquals, std::move(when_val));
      clauses.emplace_back(std::move(cond), VisitExpression(*node.children[i + 1]));
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
      if (child->kind != "Location") { operands.push_back(child.get());
}
    }
    if (operands.size() != 3) {
      throw std::runtime_error("GoogleSQL AST: BETWEEN arity");
    }
    Expression lower = BinaryExpressionExp(VisitExpression(*operands[0]),
                                           BinaryOperation::kGreaterThanEquals,
                                           VisitExpression(*operands[1]));
    Expression upper = BinaryExpressionExp(VisitExpression(*operands[0]),
                                           BinaryOperation::kLessThanEquals,
                                           VisitExpression(*operands[2]));
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
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query != nullptr) {
      return QueryExpressionExp(VisitQuery(*query), std::move(test), false,
                                negated);
    }
    for (const auto& child : node.children) {
      if (child->kind != "UnnestExpression") { continue;
}
      // `x [NOT] IN UNNEST(arr)`: three-valued membership over a runtime
      // array, reusing the quantified-comparison runtime helper.
      const GoogleSqlAstNode* inner = child->Child("ExpressionWithOptAlias");
      if (inner == nullptr || inner->children.empty()) {
        throw std::runtime_error("GoogleSQL AST: malformed UNNEST");
      }
      Expression array;
      for (const auto& expr_child : inner->children) {
        if (expr_child->kind != "Location" && expr_child->kind != "Alias" &&
            expr_child->kind != "Identifier") {
          array = VisitExpression(*expr_child);
          break;
        }
      }
      Expression result =
          FunctionCallExp("__quantified__",
                          {std::move(test), std::move(array),
                           ConstantValueExp(Value(std::string("="))),
                           ConstantValueExp(Value(std::string("ANY")))});
      return negated
                 ? UnaryExpressionExp(std::move(result), UnaryOperation::kNot)
                 : result;
    }
    throw std::runtime_error("GoogleSQL AST: IN without values");
  }
  if (node.kind == "ExpressionSubquery") {
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query == nullptr) {
      throw std::runtime_error("GoogleSQL AST: subquery without query");
    }
    // ARRAY(SELECT ...): the subquery result is consumed as one array value.
    if (node.detail == "modifier=ARRAY") {
      auto array_query = std::make_shared<QueryExpression>(
          VisitQuery(*query), nullptr, false, false);
      array_query->SetArrayResult(true);
      array_query->SetArrayElementSqlType(
          InferSubqueryArrayElementType(*query));
      return Expression(array_query);
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
        try { amount = std::stoll(str_val); } catch (...) {}
        return IntervalExpressionExp(amount, std::move(unit), std::move(str_val));
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
          try { amount = std::stoll(str_val); } catch (...) {}
          return IntervalExpressionExp(amount, std::move(unit), std::move(str_val));
        }
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        std::string col_name = expr->AsColumnValue().GetColumnName().name;
        if (HasSessionConstant(col_name)) {
          std::string str_val = GetSessionConstant(col_name);
          return IntervalExpressionExp(0, std::move(unit), std::move(str_val));
        }
        return FunctionCallExp("make_interval", {expr, ConstantValueExp(Value(std::string(unit)))});
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
          try { amount = std::stoll(str_val); } catch (...) {}
          return IntervalExpressionExp(amount, std::move(unit), std::move(str_val));
        }
      } catch (...) {}
      return FunctionCallExp("make_interval", {expr, ConstantValueExp(Value(std::string(unit)))});
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
    if (unit.starts_with("unit=")) { unit = unit.substr(5); }
    return ConstantValueExp(Value(std::move(unit)));
  }
  if (node.kind == "CastExpression") {
    if (node.children.size() < 2) {
      throw std::runtime_error("GoogleSQL AST: CAST without operand or type");
    }
    Expression child = VisitExpression(*node.children[0]);
    std::string type_name = SqlTypeFromAst(*node.children[1]);
    if (type_name.empty()) {
      if (const auto* path = node.children[1]->Child("PathExpression")) {
        type_name = Path(*path);
      } else if (node.children[1]->kind == "SimpleType") {
        for (const auto& c : node.children[1]->children) {
          type_name = SqlTypeFromAst(*c);
          if (!type_name.empty()) { break; }
        }
      }
    }
    if (type_name.empty()) { type_name = "STRING"; }
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
        if (!text_format.empty()) { text_format += " "; }
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
    struct StructFieldJson {
      std::string name;
      std::string text;
      bool is_string{false};
    };
    std::vector<StructFieldJson> fields;
    bool any_ci = false;
    size_t arg_idx = 0;
    for (const auto& child : node.children) {
      if (child->kind == "Location" || child->kind == "StructType") {
        continue;
      }
      const GoogleSqlAstNode* arg_node = child.get();
      std::string fname = arg_idx < field_names.size() ? field_names[arg_idx] : "";
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
          if (id != nullptr) { fname = Identifier(*id); }
        }
        for (const auto& arg_child : child->children) {
          if (arg_child->kind != "Location" && arg_child->kind != "Identifier" &&
              arg_child->kind != "Alias") {
            arg_node = arg_child.get();
            break;
          }
        }
      }
      if (fname.empty()) {
        fname = "f" + std::to_string(arg_idx + 1);
      }

      StructFieldJson field;
      field.name = std::move(fname);
      if (arg_node->kind == "BooleanLiteral") {
        field.text = (arg_node->detail == "TRUE" || arg_node->detail == "true")
                         ? "true"
                         : "false";
      } else if (arg_node->kind == "NullLiteral") {
        field.text = "null";
      } else {
        Expression val_expr = VisitExpression(*arg_node);
        if (val_expr) {
          try {
            Value v = val_expr->Evaluate(Row(), Schema());
            if (v.IsNull()) {
              // Keep the JSON object well-formed so downstream struct
              // parsing (UNNEST, TO_JSON_STRING) sees an explicit null.
              field.text = "null";
            } else {
              any_ci = any_ci || v.IsCaseInsensitive();
              if (v.type == ValueType::kVarChar) {
                field.text = std::string(v.value.varchar_value);
                field.is_string = true;
              } else if (v.type == ValueType::kInt64) {
                field.text = std::to_string(v.value.int_value);
              } else if (v.type == ValueType::kDouble) {
                field.text = std::to_string(v.value.double_value);
              } else {
                field.text = v.AsString();
              }
            }
          } catch (...) {}
        }
      }
      fields.push_back(std::move(field));
      ++arg_idx;
    }
    // Collation propagation: when any field value carries a case-insensitive
    // collator, the whole struct comparison folds (GoogleSQL resolves one
    // collation per comparison; struct JSON carries no per-field metadata).
    if (any_ci) {
      for (auto& field : fields) {
        if (field.is_string) {
          std::transform(field.text.begin(), field.text.end(),
                         field.text.begin(), [](unsigned char c) {
                           return c >= 'A' && c <= 'Z'
                                      ? static_cast<char>(c - 'A' + 'a')
                                      : static_cast<char>(c);
                         });
        }
      }
    }
    std::string json_out = "{";
    bool first = true;
    for (auto& field : fields) {
      if (!first) { json_out += ","; }
      first = false;
      std::string escaped_fname;
      for (char c : field.name) {
        if (c == '"') { escaped_fname += "\\\""; }
        else if (c == '\\') { escaped_fname += "\\\\"; }
        else if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
          escaped_fname += buf;
        } else {
          escaped_fname.push_back(c);
        }
      }
      json_out += "\"" + escaped_fname + "\":";
      json_out += field.is_string ? "\"" + field.text + "\"" : field.text;
    }
    json_out += "}";
    return ConstantValueExp(Value(std::move(json_out)));
  }

  if (node.kind == "DotIdentifier") {
    if (node.children.size() >= 2 && node.children[1]->kind == "Identifier") {
      Expression base_expr = VisitExpression(*node.children[0]);
      std::string field_name = Identifier(*node.children[1]);
      return FunctionCallExp("get_field", {std::move(base_expr), ConstantValueExp(Value(std::move(field_name)))});
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
    // comparison chains; subquery forms reuse the QueryExpression machinery
    // with the comparison operator and quantifier attached.
    Expression lhs;
    std::string quantifier;
    const GoogleSqlAstNode* list_node = nullptr;
    const GoogleSqlAstNode* collection = nullptr;
    const GoogleSqlAstNode* query_node = nullptr;
    for (const auto& child : node.children) {
      if (child->kind == "Location") { continue; }
      if (child->kind == "AnySomeAllOp") {
        quantifier = UpperCopy(child->detail);
        continue;
      }
      if (child->kind == "Query") {
        query_node = child.get();
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
    const QuantifierMode mode =
        is_any ? QuantifierMode::kAny : QuantifierMode::kAll;
    if (query_node != nullptr) {
      // `lhs <op> ANY/ALL (SELECT x FROM UNNEST(arr) x)` iterates the array
      // directly; evaluating it as a correlated subquery would require
      // outer-scope resolution inside UNNEST table sources.
      if (const GoogleSqlAstNode* array_expr =
              UnnestArrayOfQuantifiedSubquery(*query_node)) {
        Expression arr = VisitExpression(*array_expr);
        std::string op_text(node.detail);
        std::string quantifier_text = quantifier;
        return FunctionCallExp("__quantified__",
                               {std::move(lhs), std::move(arr),
                                ConstantValueExp(Value(std::move(op_text))),
                                ConstantValueExp(Value(std::move(quantifier_text)))});
      }
      return QueryExpressionExp(VisitQuery(*query_node), lhs, false, false,
                                op, mode);
    }
    const GoogleSqlAstNode* query =
        list_node != nullptr ? list_node->Child("Query") : nullptr;
    if (query != nullptr) {
      return QueryExpressionExp(VisitQuery(*query), lhs, false, false, op,
                                mode);
    }
    if (collection != nullptr) {
      // Generic array-collection form: evaluated by the runtime helper
      // __quantified__(lhs, array, op, mode).
      Expression arr = VisitExpression(*collection);
      std::string node_detail = node.detail;
      std::string quantifier_copy = quantifier;
      return FunctionCallExp("__quantified__",
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
    // List form shares the __quantified__ runtime with the array form so
    // three-valued combination and collation resolution stay identical
    // across all quantified-comparison shapes.
    std::string op_text(node.detail);
    std::string quantifier_text = quantifier;
    return FunctionCallExp(
        "__quantified__",
        {std::move(lhs), ArrayExpressionExp(std::move(items), ""),
         ConstantValueExp(Value(std::move(op_text))),
         ConstantValueExp(Value(std::move(quantifier_text)))});
  }

  if (node.kind == "LikeExpression") {
    // `lhs [NOT] LIKE rhs`, plus the LIKE ANY/SOME/ALL (list | query)
    // extension.
    Expression lhs;
    const GoogleSqlAstNode* any_op = nullptr;
    const GoogleSqlAstNode* list_node = nullptr;
    const GoogleSqlAstNode* query_node = nullptr;
    std::vector<Expression> operands;
    for (const auto& child : node.children) {
      if (child->kind == "Location") { continue; }
      if (child->kind == "AnySomeAllOp") {
        any_op = child.get();
        continue;
      }
      if (child->kind == "Query") {
        query_node = child.get();
        continue;
      }
      if (child->kind == "InList") {
        list_node = child.get();
        continue;
      }
      if (!lhs) {
        lhs = VisitExpression(*child);
      } else {
        operands.push_back(VisitExpression(*child));
      }
    }
    const bool negated = UpperCopy(node.detail).find("NOT") != std::string::npos;
    if (any_op != nullptr && query_node != nullptr) {
      // `lhs [NOT] LIKE ANY/ALL (subquery)`: per-row LIKE / NOT LIKE under
      // three-valued ANY/ALL combination.
      const bool is_any =
          UpperCopy(any_op->detail) == "ANY" ||
          UpperCopy(any_op->detail) == "SOME";
      return QueryExpressionExp(
          VisitQuery(*query_node), lhs, false, false,
          negated ? BinaryOperation::kNotLike : BinaryOperation::kLike,
          is_any ? QuantifierMode::kAny : QuantifierMode::kAll);
    }
    if (any_op == nullptr || list_node == nullptr) {
      if (operands.empty()) {
        throw std::runtime_error("GoogleSQL AST: malformed LIKE");
      }
      Expression call = BinaryExpressionExp(lhs, BinaryOperation::kLike,
                                            operands[0]);
      return negated ? UnaryExpressionExp(std::move(call), UnaryOperation::kNot)
                     : Expression(call);
    }
    // LIKE ANY/ALL over a pattern list.
    const bool is_any =
        UpperCopy(any_op->detail) == "ANY" ||
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

  throw std::runtime_error("GoogleSQL AST: unsupported expression " +
                           node.kind);

}


SelectSource VisitTableSource(const GoogleSqlAstNode& node, JoinType join_type,  // NOLINT(misc-no-recursion) // Recursive AST descent for nested joins/subqueries by design (see VisitQuery depth note).
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
          if (source.alias.empty()) { source.alias = Alias(*child); }
          break;
        }
      }
      if (const GoogleSqlAstNode* with_offset = node.Child("WithOffset")) {
        if (const GoogleSqlAstNode* alias = with_offset->Child("Alias")) {
          if (alias->Child("Identifier") != nullptr) {
            source.offset_alias = Identifier(*alias->Child("Identifier"));
          } else {
            source.offset_alias = "offset";
          }
        } else {
          source.offset_alias = "offset";
        }
      } else if (const GoogleSqlAstNode* with_offset2 = node.Child("WithOffsetClause")) {
        if (const GoogleSqlAstNode* alias = with_offset2->Child("Alias")) {
          if (alias->Child("Identifier") != nullptr) {
            source.offset_alias = Identifier(*alias->Child("Identifier"));
          } else {
            source.offset_alias = "offset";
          }
        } else {
          source.offset_alias = "offset";
        }
      } else {
        for (const auto& child : node.children) {
          if (child->kind.find("Offset") != std::string::npos) {
            if (const GoogleSqlAstNode* alias = child->Child("Alias")) {
              if (alias->Child("Identifier") != nullptr) {
                source.offset_alias = Identifier(*alias->Child("Identifier"));
              } else {
                source.offset_alias = "offset";
              }
            } else {
              source.offset_alias = "offset";
            }
            break;
          }
        }
      }
      if (source.alias.empty()) { source.alias = "unnest"; }
      return source;
    }
    const GoogleSqlAstNode* path = node.Child("PathExpression");
    if (path == nullptr) {
      throw std::runtime_error("GoogleSQL AST: table without path");
    }
    // GoogleSQL FROM items may be qualified field paths (`t4.array_val`,
    // `t.Info.str_value`): an implicit UNNEST of an array-typed column or
    // nested field reached through a scope alias.  Such paths never name a
    // base relation, so map them to an unnest source whose expression is
    // resolved against the enclosing scope chain at execution time.
    const std::string dotted = Path(*path);
    if (dotted.find('.') != std::string::npos &&
        dotted.back() != '.') {
      source.unnest = VisitExpression(*path);
      if (source.alias.empty()) {
        source.alias = dotted.substr(dotted.rfind('.') + 1);
      }
      return source;
    }
    source.table = dotted;
    if (source.alias.empty()) { source.alias = source.table; }
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


void AppendSources(const GoogleSqlAstNode& node, JoinType incoming,  // NOLINT(misc-no-recursion) // Recursive AST descent for nested joins by design (see VisitQuery depth note).
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
    } else if (using_clause == nullptr &&
                child->kind.starts_with("Using")) {
      using_clause = child.get();
    }
  }
  if (operands.size() != 2) {
    throw std::runtime_error("GoogleSQL AST: join arity");
  }
  AppendSources(*operands[0], incoming, std::move(condition), sources);
  JoinType type = JoinType::kInner;
  if (node.detail == "COMMA") { type = JoinType::kCross;
}
  if (node.detail == "LEFT") { type = JoinType::kLeft;
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
      Expression equality =
          BinaryExpressionExp(ColumnValueExp(Identifier(*column)),
                              BinaryOperation::kEquals,
                              ColumnValueExp(Identifier(*column)));
      join_expression =
          join_expression
              ? BinaryExpressionExp(std::move(join_expression),
                                    BinaryOperation::kAnd,
                                    std::move(equality))
              : std::move(equality);
    }
    if (!join_expression) {
      throw std::runtime_error("GoogleSQL AST: unsupported join USING clause");
    }
  }
  AppendSources(*operands[1], type, std::move(join_expression), sources);
}

std::shared_ptr<SelectStatement> VisitQuery(const GoogleSqlAstNode& query) {  // NOLINT(misc-no-recursion) // Recursive subquery/CTE traversal by design; SQL nesting is finite and parser-bounded.
  const GoogleSqlAstNode* select = (query.kind == "Select") ? &query : query.Child("Select");
  if (select == nullptr) {
    const GoogleSqlAstNode* set_op = (query.kind == "SetOperation") ? &query : query.Child("SetOperation");
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
        if (child->kind == "Query" || child->kind == "Select" || child->kind == "SetOperation") {
          operands.push_back(child.get());
        }
      }
      if (!operands.empty()) {
        auto first_stmt = VisitQuery(*operands[0]);
        for (size_t i = 1; i < operands.size(); ++i) {
          first_stmt->AddUnionAll(VisitQuery(*operands[i]));
        }
        return first_stmt;
      }
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
      select->Child("WindowClause") != nullptr
          ? select->Child("WindowClause")
          : query.Child("WindowClause");
  if (window_clause != nullptr) {
    for (const GoogleSqlAstNode* definition :
         window_clause->Children("WindowDefinition")) {
      const GoogleSqlAstNode* name_node = definition->Child("Identifier");
      const GoogleSqlAstNode* spec = definition->Child("WindowSpecification");
      if (name_node == nullptr || spec == nullptr) { continue; }
      NamedWindowParts parts;
      // `v AS (w ORDER BY z)`: inherit from the referenced definition first,
      // then overlay whatever this specification declares itself.
      if (const GoogleSqlAstNode* base = spec->Child("Identifier")) {
        const auto found = t_named_windows.find(Identifier(*base));
        if (found != t_named_windows.end()) { parts = found->second; }
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
      t_named_windows[Identifier(*name_node)] = std::move(parts);
    }
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

  const std::string upper_select_detail = UpperCopy(select->detail);
  const GoogleSqlAstNode* select_as = select->Child("SelectAs");
  const GoogleSqlAstNode* as_struct = select->Child("AsStruct");
  const bool is_as_struct = upper_select_detail.find("STRUCT") != std::string::npos ||
                            (select_as != nullptr && UpperCopy(select_as->detail).find("STRUCT") != std::string::npos) ||
                            as_struct != nullptr;
  // SELECT AS VALUE yields the bare value as the sole column; only named
  // proto/struct targets are folded into a field-list literal.
  const bool as_value = select_as != nullptr &&
                        UpperCopy(select_as->detail).find("VALUE") !=
                            std::string::npos;
  if (!is_as_struct && !as_value && (select_as != nullptr || (upper_select_detail.find("AS_MODE=") != std::string::npos && upper_select_detail.find("AS_MODE=VALUE") == std::string::npos))) {
    std::string proto_str;
    bool all_const = true;
    for (size_t i = 0; i < projections.size(); ++i) {
      std::string fname = projections[i].name.empty() ? ("f" + std::to_string(i + 1)) : projections[i].name;
      if (projections[i].expression->Type() == TypeTag::kConstantValue) {
        if (!proto_str.empty()) { proto_str += " "; }
        proto_str += fname + ": ";
        const Value& v = projections[i].expression->AsConstantValue().GetValue();
        proto_str += v.AsString();
      } else if (projections[i].expression->Type() == TypeTag::kArrayExp) {
        const auto& arr = projections[i].expression->AsArrayExpression();
        for (const auto& elem : arr.Elements()) {
          if (elem->Type() == TypeTag::kConstantValue) {
            if (!proto_str.empty()) { proto_str += " "; }
            proto_str += fname + ": " + elem->AsConstantValue().GetValue().AsString();
          } else {
            all_const = false;
          }
        }
      } else {
        all_const = false;
      }
    }
    if (all_const) {
      projections = {NamedExpression("", ConstantValueExp(Value(std::move(proto_str))))};
    }
  }

  std::vector<SelectSource> sources;

  std::vector<std::string> tables;
  if (const GoogleSqlAstNode* from = select->Child("FromClause")) {
    for (const auto& child : from->children) {
      AppendSources(*child, JoinType::kCross, nullptr, &sources);
    }
    for (const SelectSource& source : sources) {
      if (!source.table.empty()) { tables.push_back(source.table);
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
      if (!parsed.expression) { continue; }
      order_by.push_back({std::move(parsed.expression), parsed.ascending,
                          parsed.nulls_first});
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
      if (child->kind == "IntLiteral") { offset = ParseUnsignedLiteral(*child);
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
    for (const GoogleSqlAstNode* item : group->Children("GroupingItem")) {
      if (!item->children.empty()) {
        expressions.push_back(VisitExpression(*item->children[0]));
      }
    }
    statement->SetGroupBy(std::move(expressions));
  }
  if (const GoogleSqlAstNode* having = select->Child("Having")) {
    if (!having->children.empty()) {
      statement->SetHaving(VisitExpression(*having->children[0]));
    }
  }
  const GoogleSqlAstNode* qualify =
      select->Child("Qualify") != nullptr ? select->Child("Qualify")
                                          : query.Child("Qualify");
  if (qualify != nullptr) {
    if (!qualify->children.empty()) {
      statement->SetQualify(VisitExpression(*qualify->children[0]));
    }
  }
  if (const GoogleSqlAstNode* with = query.Child("WithClause")) {
    for (const GoogleSqlAstNode* entry : with->Children("WithClauseEntry")) {
      const GoogleSqlAstNode* aliased = entry->Child("AliasedQuery");
      if (aliased == nullptr) { continue;
}
      const GoogleSqlAstNode* name = aliased->Child("Identifier");
      const GoogleSqlAstNode* nested = aliased->Child("Query");
      if (name != nullptr && nested != nullptr) {
        statement->AddWithQuery(Identifier(*name), VisitQuery(*nested));
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
        NeedsRelationalEvaluation(source.join_condition)) {
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
  // SELECT AS STRUCT / AS VALUE shape the projected value: STRUCT rows are
  // consumed by outer expressions as whole struct values (encoded like the
  // struct constructor output), VALUE strips the single column.
  statement->SetAsStruct(is_as_struct && !as_value);
  // Sort keys and grouping keys are evaluated by the plan executor with a
  // plain AST walk, so query expressions (EXISTS / scalar subqueries) there
  // must route to the relational interpreter, which resolves them against
  // the enclosing scope chain.
  for (const auto& term : statement->OrderBy()) {
    if (NeedsRelationalEvaluation(term.expression)) {
      statement->MarkComplex();
      break;
    }
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
  if (type == "date") { return ValueType::kDate;
}
  if (type == "string" || type == "varchar" || type == "char" ||
      type == "timestamp" || type == "datetime") {
    return ValueType::kVarChar;
  }
    throw std::runtime_error("GoogleSQL AST: unsupported column type " + type);
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

InsertMode InsertModeFromDetail(const std::string& detail) {
  if (detail.find("insert_mode=IGNORE") != std::string::npos) {
    return InsertMode::kIgnore;
  }
  if (detail.find("insert_mode=REPLACE") != std::string::npos) {
    return InsertMode::kReplace;
  }
  if (detail.find("insert_mode=UPDATE") != std::string::npos) {
    return InsertMode::kUpdate;
  }
  return InsertMode::kDefault;
}

int64_t AssertRowsModifiedValue(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* literal = node.Child("IntLiteral");
  if (literal == nullptr) {
    throw std::runtime_error(
        "GoogleSQL AST: ASSERT_ROWS_MODIFIED without count");
  }
  return ParseIntLiteral(*literal);
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
  std::vector<std::vector<Expression>> rows;
  const GoogleSqlAstNode* row_list = root.Child("InsertValuesRowList");
  if (row_list != nullptr) {
    for (const GoogleSqlAstNode* row : row_list->Children("InsertValuesRow")) {
      std::vector<Expression> values;
      for (const auto& value : row->children) {
        if (value->kind == "Location" || value->kind == "Hint") { continue;
}
        values.push_back(VisitExpression(*value));
      }
      rows.push_back(std::move(values));
    }
  }
  auto statement = std::make_unique<InsertStatement>(
      Path(*path), std::move(rows), std::move(columns));
  statement->SetMode(InsertModeFromDetail(root.detail));
  if (const GoogleSqlAstNode* query = root.Child("Query")) {
    statement->SetQuery(VisitQuery(*query));
  }
  if (const GoogleSqlAstNode* assert = root.Child("AssertRowsModified")) {
    statement->SetAssertRowsModified(AssertRowsModifiedValue(*assert));
  }
  return statement;
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
      throw std::runtime_error("GoogleSQL AST: bad UPDATE assignment");
    }
    assignments.emplace_back(ColumnName(Path(*set->children[0])),
                             VisitExpression(*set->children[1]));
  }
  // The WHERE clause is the single remaining child once the target path and
  // the assignment list are removed. Table aliases, ASSERT_ROWS_MODIFIED,
  // THEN RETURN and UPDATE...FROM siblings are recognized but not mapped
  // (returning/join-update are gated features); they must not masquerade as
  // WHERE candidates.
  std::vector<const GoogleSqlAstNode*> where_candidates;
  for (const auto& child : root.children) {
    if (child->kind == "PathExpression" || child->kind == "UpdateItemList" ||
        child->kind == "Location" || child->kind == "Hint" ||
        child->kind == "Alias" || child->kind == "AssertRowsModified" ||
        child->kind == "ReturningClause" ||
        child->kind == "FromClause") {
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
  if (const GoogleSqlAstNode* assert = root.Child("AssertRowsModified")) {
    statement->SetAssertRowsModified(AssertRowsModifiedValue(*assert));
  }
  return statement;
}

std::unique_ptr<Statement> VisitDelete(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    throw std::runtime_error("GoogleSQL AST: bad DELETE");
}
  std::vector<const GoogleSqlAstNode*> where_candidates;
  for (const auto& child : root.children) {
    if (child->kind == "PathExpression" || child->kind == "Location" ||
        child->kind == "Hint" || child->kind == "Alias" ||
        child->kind == "AssertRowsModified" ||
        child->kind == "ReturningClause" ||
        child->kind == "FromClause") {
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
  if (const GoogleSqlAstNode* assert = root.Child("AssertRowsModified")) {
    statement->SetAssertRowsModified(AssertRowsModifiedValue(*assert));
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
    proj.emplace_back("constant", ConstantValueExp(Value(std::string(const_name))));
    return std::make_unique<SelectStatement>(
        std::move(proj),
        std::vector<std::string>{},
        Expression{});
  }
  if (root.kind == "CreateTableStatement") { return VisitCreate(root);
}
  if (root.kind == "InsertStatement") { return VisitInsert(root);
}
  if (root.kind == "UpdateStatement") { return VisitUpdate(root);
}
  if (root.kind == "DeleteStatement") { return VisitDelete(root);
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
