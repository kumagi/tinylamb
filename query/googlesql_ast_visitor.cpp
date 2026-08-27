/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/googlesql_ast_visitor.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <limits>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/proto_schema.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "database/transaction_context.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/sql_udf.hpp"
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

std::string SqlTypeFromAst(const GoogleSqlAstNode& node);
std::string InferSubqueryArrayElementType(const GoogleSqlAstNode& query_node);
std::string InferAggregateArrayElementType(const GoogleSqlAstNode& node);

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
// Hex literals (0x / 0X) are accepted up to 16 digits; values above INT64_MAX
// are GoogleSQL UINT64 literals kept as their two's-complement bit pattern.
int64_t ParseIntLiteral(const GoogleSqlAstNode& node) {
  const std::string& text = node.detail;
  // Hex literals (0x1F, 0XFFFFFFFFFFFFD8F0) are bit patterns: they wrap
  // modulo 2^64 exactly like the reference engine, where 0xFF... == -1.
  if (text.size() > 2 && text[0] == '0' &&
      (text[1] == 'x' || text[1] == 'X')) {
    const std::string_view digits(text.data() + 2, text.size() - 2);
    const bool hex_only =
        !digits.empty() &&
        std::ranges::all_of(digits, [](char c) {
          return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f') ||
                 ('A' <= c && c <= 'F');
        });
    if (!hex_only) {
      throw std::runtime_error("GoogleSQL AST: malformed integer literal " +
                               text);
    }
    uint64_t magnitude = 0;
    const auto* end = digits.data() + digits.size();
    const auto [ptr, ec] =
        std::from_chars(digits.data(), end, magnitude, 16);
    if (ec != std::errc() || ptr != end) {
      throw std::runtime_error("GoogleSQL AST: integer literal out of range " +
                               text);
    }
    return static_cast<int64_t>(magnitude);
  }
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
  auto [ptr, ec] = std::from_chars(text.data(), end, value);
  if (ec == std::errc::result_out_of_range) {
    // Literals above INT64_MAX name UINT64 values; the engine stores them
    // as their two's-complement signed bit pattern.
    uint64_t magnitude = 0;
    const auto [u_ptr, u_ec] =
        std::from_chars(text.data(), end, magnitude, 10);
    if (u_ec == std::errc() && u_ptr == end) {
      return static_cast<int64_t>(magnitude);
    }
  }
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
  // std::stod rejects subnormal magnitudes on some libstdc++ versions
  // (ERANGE); strtod accepts the full IEEE-754 double domain.
  errno = 0;
  const std::string text = node.detail;
  char* end = nullptr;
  const double value = std::strtod(text.c_str(), &end);
  if (end == text.c_str() || *end != '\0') {
    throw std::runtime_error("GoogleSQL AST: float literal out of range " +
                             text);
  }
  // strtod reports ERANGE for subnormal (denormal) results; the returned
  // value is still the closest representable double and is accepted here.
  if (std::isinf(value)) {
    throw std::runtime_error("GoogleSQL AST: float literal out of range " +
                             text);
  }
  return value;
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

// --- SQL UDF support (CREATE [TEMP] [AGGREGATE] FUNCTION) -------------------
//
// The visitor keeps a process-wide registry of user-defined SQL functions,
// mirroring the session-scoped TEMP semantics of the single-connection
// compliance harness (the same pattern as SetSessionConstant).  Definitions
// store the raw body AST; scalar bodies are compiled lazily on first call and
// served from the expression-layer runtime registry, while aggregate bodies
// are spliced into every call site so the enclosing statement's aggregation
// machinery computes their inner aggregates.

Expression VisitExpression(const GoogleSqlAstNode& node);

struct UdfParameter {
  std::string name;
  // DEFAULT expression subtree; null when the parameter is required.
  std::unique_ptr<GoogleSqlAstNode> default_value;
};


constexpr int kMaxUdfExpansionDepth = 64;

thread_local int tls_udf_expansion_depth = 0;

class UdfExpansionDepthGuard {
 public:
  UdfExpansionDepthGuard() {
    if (tls_udf_expansion_depth >= kMaxUdfExpansionDepth) {
      throw std::runtime_error("SQL UDF invocation depth exceeds " +
                               std::to_string(kMaxUdfExpansionDepth));
    }
    ++tls_udf_expansion_depth;
  }
  ~UdfExpansionDepthGuard() { --tls_udf_expansion_depth; }
  UdfExpansionDepthGuard(const UdfExpansionDepthGuard&) = delete;
  UdfExpansionDepthGuard& operator=(const UdfExpansionDepthGuard&) = delete;
};

Expression SubstituteParameters(
    const Expression& expression,
    const std::unordered_map<std::string, Expression>& bindings);

std::shared_ptr<SelectStatement> SubstituteInSelect(
    const SelectStatement& select,
    const std::unordered_map<std::string, Expression>& bindings);

WindowOrderTerm SubstituteInOrderTerm(
    const WindowOrderTerm& term,
    const std::unordered_map<std::string, Expression>& bindings) {
  WindowOrderTerm result;
  result.ascending = term.ascending;
  result.nulls_first = term.nulls_first;
  if (term.expression) {
    result.expression = SubstituteParameters(term.expression, bindings);
  }
  return result;
}

Expression SubstituteParameters(
    const Expression& expression,
    const std::unordered_map<std::string, Expression>&
        bindings) {  // NOLINT(misc-no-recursion) // AST-shaped tree walk; depth
                     // bounded by the parser's expression nesting.
  if (!expression) {
    return expression;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      if (!column.schema.empty() || column.name == "*") {
        return expression;
      }
      std::string lower_name = column.name;
      for (char& c : lower_name) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      const auto found = bindings.find(lower_name);
      if (found == bindings.end()) {
        return expression;
      }
      return found->second;
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      return BinaryExpressionExp(
          SubstituteParameters(binary.Left(), bindings), binary.Op(),
          SubstituteParameters(binary.Right(), bindings));
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      return UnaryExpressionExp(SubstituteParameters(unary.Child(), bindings),
                                unary.Op());
    }
    case TypeTag::kAggregateExp: {
      const auto& aggregate = expression->AsAggregateExpression();
      auto rebuilt = std::make_shared<AggregateExpression>(
          aggregate.GetType(),
          SubstituteParameters(aggregate.Child(), bindings),
          aggregate.Distinct());
      if (aggregate.Having() != AggregateHavingModifier::kNone) {
        rebuilt->SetHaving(
            aggregate.Having(),
            SubstituteParameters(aggregate.HavingCondition(), bindings));
      }
      if (aggregate.WhereFilter()) {
        rebuilt->SetWhereFilter(
            SubstituteParameters(aggregate.WhereFilter(), bindings));
      }
      if (aggregate.SecondaryArg()) {
        rebuilt->SetSecondaryArg(
            SubstituteParameters(aggregate.SecondaryArg(), bindings));
      }
      if (!aggregate.TrailingArgs().empty()) {
        std::vector<Expression> trailing;
        trailing.reserve(aggregate.TrailingArgs().size());
        for (const Expression& arg : aggregate.TrailingArgs()) {
          trailing.push_back(SubstituteParameters(arg, bindings));
        }
        rebuilt->SetTrailingArgs(std::move(trailing));
      }
      if (!aggregate.InnerOrderBy().empty()) {
        std::vector<WindowOrderTerm> order;
        order.reserve(aggregate.InnerOrderBy().size());
        for (const WindowOrderTerm& term : aggregate.InnerOrderBy()) {
          order.push_back(SubstituteInOrderTerm(term, bindings));
        }
        rebuilt->SetInnerOrderBy(std::move(order));
      }
      rebuilt->SetInnerLimit(aggregate.InnerLimit());
      return rebuilt;
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(searched.when_clauses_.size());
      for (const auto& clause : searched.when_clauses_) {
        clauses.emplace_back(SubstituteParameters(clause.first, bindings),
                             SubstituteParameters(clause.second, bindings));
      }
      return CaseExpressionExp(
          std::move(clauses),
          SubstituteParameters(searched.else_clause_, bindings));
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      std::vector<Expression> list;
      list.reserve(in.list_.size());
      for (const Expression& item : in.list_) {
        list.push_back(SubstituteParameters(item, bindings));
      }
      return InExpressionExp(SubstituteParameters(in.child_, bindings),
                             std::move(list));
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      std::vector<Expression> args;
      args.reserve(call.Args().size());
      for (const Expression& arg : call.Args()) {
        args.push_back(SubstituteParameters(arg, bindings));
      }
      return FunctionCallExp(call.FuncName(), std::move(args));
    }
    case TypeTag::kArrayExp: {
      const auto& array = expression->AsArrayExpression();
      std::vector<Expression> elements;
      elements.reserve(array.Elements().size());
      for (const Expression& element : array.Elements()) {
        elements.push_back(SubstituteParameters(element, bindings));
      }
      return ArrayExpressionExp(std::move(elements), array.ElementSqlType());
    }
    case TypeTag::kCastExp: {
      const auto& cast = expression->AsCastExpression();
      return CastExpressionExp(SubstituteParameters(cast.Child(), bindings),
                               cast.TargetTypeName(), cast.ReturnNullOnError());
    }
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      auto rebuilt = std::make_shared<QueryExpression>(
          SubstituteInSelect(*query.Query(), bindings),
          SubstituteParameters(query.Test(), bindings), query.Exists(),
          query.Negated(), query.Op(), query.Mode());
      rebuilt->SetArrayResult(query.ArrayResult());
      return Expression(rebuilt);
    }
    default:
      // Constants and intervals carry no parameter references.
      return expression;
  }
}

std::shared_ptr<SelectStatement> SubstituteInSelect(
    const SelectStatement& select,
    const std::unordered_map<std::string, Expression>&
        bindings) {  // NOLINT(misc-no-recursion) // Mirrors statement-tree
                     // binding in sql_template.cpp.
  std::vector<NamedExpression> items;
  items.reserve(select.SelectList().size());
  for (const NamedExpression& item : select.SelectList()) {
    items.emplace_back(item.name,
                       SubstituteParameters(item.expression, bindings));
  }
  auto result = std::make_shared<SelectStatement>(
      std::move(items), select.FromClause(),
      SubstituteParameters(select.WhereClause(), bindings),
      std::vector<SelectStatement::OrderByTerm>{}, select.Limit(),
      select.Offset(), select.Distinct());
  result->SetLimit(select.HasLimit() ? std::optional<size_t>(select.Limit())
                                     : std::nullopt);
  std::vector<SelectSource> sources;
  sources.reserve(select.Sources().size());
  for (const SelectSource& source : select.Sources()) {
    SelectSource copied = source;
    copied.join_condition =
        SubstituteParameters(source.join_condition, bindings);
    if (source.query) {
      copied.query = SubstituteInSelect(*source.query, bindings);
    }
    sources.push_back(std::move(copied));
  }
  result->SetSources(std::move(sources));
  for (const auto& [alias, table] : select.Aliases()) {
    result->AddAlias(alias, table);
  }
  if (!select.GroupBy().empty()) {
    std::vector<Expression> group;
    group.reserve(select.GroupBy().size());
    for (const Expression& item : select.GroupBy()) {
      group.push_back(SubstituteParameters(item, bindings));
    }
    result->SetGroupBy(std::move(group));
  }
  if (select.Having()) {
    result->SetHaving(SubstituteParameters(select.Having(), bindings));
  }
  if (select.Qualify()) {
    result->SetQualify(SubstituteParameters(select.Qualify(), bindings));
  }
  if (!select.OrderBy().empty()) {
    std::vector<SelectStatement::OrderByTerm> order;
    order.reserve(select.OrderBy().size());
    for (const SelectStatement::OrderByTerm& term : select.OrderBy()) {
      order.push_back({SubstituteParameters(term.expression, bindings),
                       term.ascending, term.nulls_first});
    }
    result->SetOrderBy(std::move(order));
  }
  for (const auto& [name, query] : select.WithQueries()) {
    result->AddWithQuery(name, SubstituteInSelect(*query, bindings));
  }
  if (select.RequiresRelationalEvaluation()) {
    result->MarkComplex();
  }
  result->SetAsStruct(select.AsStruct());
  return result;
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

// Recognizes the correlated shape `SELECT x FROM UNNEST(<expr>) [AS x]`
// (no other clauses) used by quantified comparisons over table arrays and
// returns the array expression node.  Returns nullptr for anything else.
const GoogleSqlAstNode*
UnnestArrayOfQuantifiedSubquery(  // NOLINT(misc-no-recursion)
    const GoogleSqlAstNode& query) {
  const GoogleSqlAstNode* select =
      (query.kind == "Select") ? &query : query.Child("Select");
  if (select == nullptr) {
    return nullptr;
  }
  for (const auto& child : select->children) {
    if (child->kind == "Location") {
      continue;
    }
    if (child->kind != "SelectList" && child->kind != "FromClause") {
      return nullptr;
    }
  }
  const GoogleSqlAstNode* select_list = select->Child("SelectList");
  if (select_list == nullptr || select_list->children.size() != 1) {
    return nullptr;
  }
  const GoogleSqlAstNode* from = select->Child("FromClause");
  if (from == nullptr || from->children.size() != 1) {
    return nullptr;
  }
  const GoogleSqlAstNode* source = from->children.front().get();
  if (source == nullptr || source->kind != "TablePathExpression") {
    return nullptr;
  }
  const GoogleSqlAstNode* unnest = source->Child("UnnestExpression");
  if (unnest == nullptr) {
    return nullptr;
  }
  if (source->Child("WithOffset") != nullptr ||
      source->Child("WithOffsetClause") != nullptr) {
    return nullptr;
  }
  const GoogleSqlAstNode* expr_with_alias =
      unnest->Child("ExpressionWithOptAlias");
  if (expr_with_alias == nullptr) {
    return nullptr;
  }
  for (const auto& child : expr_with_alias->children) {
    if (child->kind != "Location" && child->kind != "Identifier" &&
        child->kind != "Alias") {
      return child.get();
    }
  }
  return nullptr;
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
  if (detail == "<<") {
    return BinaryOperation::kShiftLeft;
  }
  if (detail == ">>") {
    return BinaryOperation::kShiftRight;
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

std::shared_ptr<SelectStatement> VisitQuery(const GoogleSqlAstNode& query);
Expression ExpandUdfCall(std::string name, std::vector<Expression> arguments);
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
      return std::ranges::any_of(
          expression->AsArrayExpression().Elements(),
          [](const Expression& element) {
            return NeedsRelationalEvaluation(element, false);
          });
    case TypeTag::kColumnValue: {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      // Deep field paths (`t.Info.str_value`, `sub.ca.a`) resolve through
      // the relational interpreter's nested-field Lookup; the plan
      // executor's plain AST walk only knows flat columns.
      return name.schema.find('.') != std::string::npos ||
             name.name.find('.') != std::string::npos;
    }
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

// ---------------------------------------------------------------------------
// SQL UDF / UDA support (CREATE TEMP FUNCTION / CREATE TEMP AGGREGATE
// FUNCTION).  Definitions are kept in a session registry; call sites expand
// by re-visiting the body AST with parameter identifiers substituted by the
// argument expressions.  Substitution is shadow-aware: a parameter whose name
// is explicitly bound by an intervening query block (derived-table output,
// UNNEST alias, ...) resolves to that binding instead of the parameter.
struct SqlUdf {
  std::string name;
  std::vector<std::pair<std::string, bool>>
      parameters;  // (lower name, NOT AGGREGATE)
  bool is_aggregate{false};
  std::shared_ptr<GoogleSqlAstNode> root;  // owns the AST copy
  const GoogleSqlAstNode* body{nullptr};
  // Body analysis: how often each parameter occurs, and whether the body is
  // too complex (subqueries, aggregates) for argument-once binding.
  std::vector<size_t> parameter_counts;
  bool simple_body{true};
};

std::unordered_map<std::string, SqlUdf>& UdfRegistry() {
  static thread_local std::unordered_map<std::string, SqlUdf> registry;
  return registry;
}

// TEMP views created by CREATE VIEW statements: name -> cloned statement
// AST. Views expand as macro subqueries at FROM-reference time.
std::unordered_map<std::string, std::shared_ptr<GoogleSqlAstNode>>&
ViewRegistry() {
  static thread_local std::unordered_map<std::string,
                                         std::shared_ptr<GoogleSqlAstNode>>
      registry;
  return registry;
}

size_t& ViewExpansionDepth() {
  static thread_local size_t depth = 0;
  return depth;
}

std::unique_ptr<GoogleSqlAstNode> CloneAstNode(
    const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion)
  auto clone = std::make_unique<GoogleSqlAstNode>();
  clone->kind = node.kind;
  clone->detail = node.detail;
  clone->start = node.start;
  clone->end = node.end;
  clone->children.reserve(node.children.size());
  for (const auto& child : node.children) {
    clone->children.push_back(CloneAstNode(*child));
  }
  return clone;
}

struct UdfExpansionFrame {
  const SqlUdf* udf;
  const std::vector<Expression>* arguments;
  size_t mask_depth;
};

thread_local std::vector<UdfExpansionFrame> t_udf_frames;
thread_local std::vector<std::unordered_set<std::string>> t_udf_bound_masks;

// Stable storage for synthetic lambda-expansion frames (see
// RewriteArrayTransformLambda): deque so push/pop never moves elements.
std::deque<SqlUdf>& t_lambda_frame_storage() {
  static thread_local std::deque<SqlUdf> storage;
  return storage;
}

// CREATE TEMP CONSTANT values that could not be folded to text (subqueries,
// function calls): resolved per reference through the relational evaluator.
std::unordered_map<std::string, Expression>& SessionConstantExpressions() {
  static thread_local std::unordered_map<std::string, Expression> constants;
  return constants;
}

// Returns the argument expression bound to `path_name` by the innermost
// active expansion, or nullptr when the identifier must resolve normally
// (no parameter of that name, or an explicit binding shadows it).  A dotted
// path whose leading segment names a parameter (`e.x` for a STRUCT-bound
// lambda element) substitutes the base and traverses the remaining fields.
Expression SubstituteUdfParameter(const std::string& path_name) {
  if (t_udf_frames.empty()) {
    return nullptr;
  }
  const size_t dot = path_name.find('.');
  const std::string head =
      dot == std::string::npos ? path_name : path_name.substr(0, dot);
  const std::string lower = Lower(head);
  // Walk masks top-down alongside frames: masks pushed above a frame's
  // mask_depth belong to query blocks between the expansion and this
  // identifier occurrence.
  size_t mask_pos = t_udf_bound_masks.size();
  for (size_t f = t_udf_frames.size(); f-- > 0;) {
    const UdfExpansionFrame& frame = t_udf_frames[f];
    while (mask_pos > frame.mask_depth) {
      --mask_pos;
      if (t_udf_bound_masks[mask_pos].count(lower) != 0) {
        // Explicitly bound between this expansion and the occurrence: the
        // column/alias wins over every enclosing parameter.
        return nullptr;
      }
    }
    if (frame.arguments == nullptr) {
      continue;
    }
    for (size_t index = 0; index < frame.udf->parameters.size(); ++index) {
      if (frame.udf->parameters[index].first == lower) {
        Expression base = (*frame.arguments)[index];
        if (dot == std::string::npos) {
          return base;
        }
        // Field traversal on a substituted STRUCT/PROTO base (`e.x`): safe
        // access so anonymous struct members surface as NULL, matching the
        // reference lambda semantics.
        std::string remaining = path_name.substr(dot + 1);
        while (!remaining.empty()) {
          const size_t segment_end = remaining.find('.');
          std::string field = segment_end == std::string::npos
                                  ? remaining
                                  : remaining.substr(0, segment_end);
          base = FunctionCallExp(
              "__get_field_safe",
              {std::move(base), ConstantValueExp(Value(std::move(field)))});
          if (segment_end == std::string::npos) { break; }
          remaining = remaining.substr(segment_end + 1);
        }
        return base;
      }
    }
  }
  return nullptr;
}

// Analyzes a registered body: per-parameter identifier occurrence counts and
// whether subqueries/aggregates rule out argument-once binding.
void AnalyzeUdfBody(const GoogleSqlAstNode& node,  // NOLINT(misc-no-recursion)
                    const std::vector<std::pair<std::string, bool>>& parameters,
                    std::vector<size_t>* counts, bool* simple) {
  static const std::unordered_set<std::string> kAggregateNames = {
      "count",   "sum",       "avg",        "min",         "max",
      "countif", "array_agg", "string_agg", "logical_and", "logical_or"};
  if (node.kind == "Query" || node.kind == "ExpressionSubquery" ||
      node.kind == "TableSubquery" || node.kind == "AnalyticFunctionCall") {
    *simple = false;
    return;
  }
  if (node.kind == "PathExpression" && node.children.size() == 1 &&
      node.children[0]->kind == "Identifier") {
    const std::string lower = Lower(Identifier(*node.children[0]));
    for (size_t i = 0; i < parameters.size(); ++i) {
      if (parameters[i].first == lower) {
        ++(*counts)[i];
      }
    }
    return;
  }
  if (node.kind == "FunctionCall" && !node.children.empty() &&
      node.children.front()->kind == "PathExpression") {
    try {
      const std::string fn = Lower(Path(*node.children.front()));
      if (kAggregateNames.count(fn) != 0) {
        *simple = false;
      }
    } catch (...) {
    }
  }
  for (const auto& child : node.children) {
    AnalyzeUdfBody(*child, parameters, counts, simple);
  }
}

// Expands a call to a registered SQL function by visiting its body with the
// call arguments substituted for the parameters. Aggregate definitions whose
// expanded body contains no aggregate still force grouped semantics so that
// they yield exactly one row per group even over empty input.

// Collects the names explicitly bound by a FROM clause subtree (derived
// table outputs, UNNEST / table aliases, WITH OFFSET aliases). Plain base
// tables bind nothing here because their column sets are unknown at visit
// time.
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

// A HintEntry is either "name=value" (one leading Identifier) or
// "engine.name=value" (two). Qualified hints target another engine and are
// ignored; unqualified hints must be recognized by the engine, and none are.
void RejectUnsupportedHints(  // NOLINT(misc-no-recursion)
    const GoogleSqlAstNode& node) {
  if (node.kind == "HintEntry") {
    size_t leading_identifiers = 0;
    for (const auto& child : node.children) {
      if (child->kind != "Identifier") { break; }
      ++leading_identifiers;
    }
    if (leading_identifiers <= 1) {
      throw std::runtime_error(
          std::string("Unsupported hint: ") +
          (node.children.empty() ? std::string("")
                                 : Identifier(*node.children.front())));
    }
  }
  for (const auto& child : node.children) {
    RejectUnsupportedHints(*child);
  }
}

// Strict UTF-8 validator: BYTES -> STRING casts must reject the sequences
// GoogleSQL refuses (surrogates, overlong forms, truncated tails) instead of
// passing mojibake through.
bool IsValidUtf8Text(std::string_view text) {
  size_t i = 0;
  while (i < text.size()) {
    const uint8_t lead = static_cast<uint8_t>(text[i]);
    size_t continuation_count = 0;
    uint8_t second_lower = 0x80;
    uint8_t second_upper = 0xBF;
    if (lead < 0x80) {
      ++i;
      continue;
    } else if (lead >= 0xC2 && lead < 0xDF) {
      continuation_count = 1;
    } else if (lead >= 0xE0 && lead < 0xF0) {
      continuation_count = 2;
      second_lower = lead == 0xE0 ? 0xA0 : 0x80;
      second_upper = lead == 0xED ? 0x9F : 0xBF;
    } else if (lead >= 0xF0 && lead < 0xF5) {
      continuation_count = 3;
      second_lower = lead == 0xF0 ? 0x90 : 0x80;
      second_upper = lead == 0xF4 ? 0x8F : 0xBF;
    } else {
      return false;
    }
    if (i + continuation_count >= text.size()) { return false; }
    for (size_t c = 1; c <= continuation_count; ++c) {
      const uint8_t byte = static_cast<uint8_t>(text[i + c]);
      const uint8_t lower = c == 1 ? second_lower : 0x80;
      const uint8_t upper = c == 1 ? second_upper : 0xBF;
      if (byte < lower || byte > upper) { return false; }
    }
    i += continuation_count + 1;
  }
  return true;
}

bool IsAstTrivia(std::string_view kind);

// Builds NEW <proto>(field AS value, ...) into the engine's text-format
// representation, validating registry-known messages along the way.
Expression BuildNewConstructor(const GoogleSqlAstNode& node);

// Validates SELECT AS <proto> projections against registry field metadata;
// wraps non-constant enum-typed values in a runtime validation guard.
void ValidateSelectAsProjections(
    const std::string& message_name,
    const std::vector<std::pair<std::string, const GoogleSqlAstNode*>>& named,
    std::vector<NamedExpression>* projections);

// GoogleSQL coerces STRUCT<...> casts field-by-field and raises when a field
// value cannot be coerced to the target field type. The engine's legacy cast
// path collapses struct types, losing that validation; this re-establishes
// it for literal struct sources. Anything not statically checkable is left
// to the existing runtime behavior untouched.
void ValidateStructCastCoercibility(const GoogleSqlAstNode& node,
                                    bool safe) {
  (void)safe;
  if (node.children.size() < 2 ||
      node.children[1]->kind != "StructType") {
    return;
  }
  const GoogleSqlAstNode& source = *node.children[0];
  if (source.kind != "StructConstructorWithParens" &&
      source.kind != "StructConstructorWithKeyword" &&
      source.kind != "StructConstructorWithType") {
    return;
  }
  std::vector<std::string> field_types;
  for (const auto& field : node.children[1]->children) {
    if (field->kind != "StructField") { continue; }
    std::string field_type;
    for (const auto& part : field->children) {
      field_type = SqlTypeFromAst(*part);
      if (!field_type.empty()) { break; }
    }
    field_types.push_back(field_type);
  }
  std::vector<const GoogleSqlAstNode*> elements;
  for (const auto& child : source.children) {
    if (IsAstTrivia(child->kind)) { continue; }
    // Keyword constructors wrap each field in a StructConstructorArg node.
    if (child->kind == "StructConstructorArg") {
      for (const auto& inner : child->children) {
        if (IsAstTrivia(inner->kind)) { continue; }
        elements.push_back(inner.get());
        break;
      }
      continue;
    }
    elements.push_back(child.get());
  }
  if (field_types.empty() || elements.size() != field_types.size()) {
    return;
  }
  Row dummy_row;
  Schema dummy_schema;
  for (size_t i = 0; i < elements.size(); ++i) {
    Expression element_expr;
    try {
      element_expr = VisitExpression(*elements[i]);
      Value original = element_expr->Evaluate(dummy_row, dummy_schema);
      if (original.IsNull()) { continue; }
      if (field_types[i].empty()) { continue; }
      Expression coerced = CastExpressionExp(
          element_expr, field_types[i], /*return_null_on_error=*/true);
      if (coerced->Evaluate(dummy_row, dummy_schema).IsNull()) {
        throw std::runtime_error("Cannot coerce struct field " +
                                 std::to_string(i + 1) + " to " +
                                 field_types[i]);
      }
    } catch (const std::runtime_error&) {
      throw;
    } catch (const std::exception&) {
      // Not statically decidable: defer to the legacy runtime path.
      return;
    }
  }
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
  for (const auto& child : term->children) {
    if (child->kind == "Location") {
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

// Shared lambda plumbing for the higher-order array functions: extracts the
// parameter names, binds them to the synthetic UNNEST bindings, and visits
// the body under that expansion frame.
std::string InferArrayElementSqlType(const GoogleSqlAstNode& node);

struct LambdaBindingResult {
  Expression body;
  std::string element_binding;
  std::string offset_binding;
  size_t param_count{0};
};

LambdaBindingResult BindLambdaBody(const GoogleSqlAstNode& lambda) {
  std::vector<std::string> params;
  for (size_t i = 0; i + 1 < lambda.children.size(); ++i) {
    const GoogleSqlAstNode& head = *lambda.children[i];
    if (head.kind == "PathExpression") {
      params.push_back(Lower(Path(head)));
    } else if (head.kind == "StructConstructorWithParens") {
      for (const auto& sub : head.children) {
        if (sub->kind == "PathExpression") {
          params.push_back(Lower(Path(*sub)));
        }
      }
    }
  }
  const GoogleSqlAstNode* body =
      lambda.children.empty() ? nullptr : lambda.children.back().get();
  // A bare-parameter body (`e -> e`, `e -> e.x`) is a valid projection and
  // resolves through ordinary parameter substitution.
  if (params.empty() || params.size() > 2 || body == nullptr) {
    throw std::runtime_error("GoogleSQL AST: unsupported Lambda");
  }
  static thread_local size_t t_lambda_counter = 0;
  const size_t lambda_id = ++t_lambda_counter;
  LambdaBindingResult result;
  result.element_binding = "__lambda_element_" + std::to_string(lambda_id);
  result.offset_binding = "__lambda_offset_" + std::to_string(lambda_id);
  result.param_count = params.size();
  // The frame storage keeps stable SqlUdf addresses while nested lambdas
  // expand; entries are popped with their frames.
  auto& frame_udf = t_lambda_frame_storage().emplace_back();
  frame_udf.name = "__lambda";
  for (const std::string& param : params) {
    frame_udf.parameters.emplace_back(param, false);
  }
  std::vector<Expression> bound;
  bound.push_back(ColumnValueExp(ColumnName("", result.element_binding)));
  if (params.size() == 2) {
    bound.push_back(ColumnValueExp(ColumnName("", result.offset_binding)));
  }
  const UdfExpansionFrame frame{&frame_udf, &bound, t_udf_bound_masks.size()};
  t_udf_frames.push_back(frame);
  try {
    result.body = VisitExpression(*body);
  } catch (...) {
    t_udf_frames.pop_back();
    t_lambda_frame_storage().pop_back();
    throw;
  }
  t_udf_frames.pop_back();
  t_lambda_frame_storage().pop_back();
  return result;
}

SelectSource MakeLambdaSource(Expression array_expr,
                              const LambdaBindingResult& binding) {
  SelectSource source;
  source.unnest = std::move(array_expr);
  source.alias = binding.element_binding;
  if (binding.param_count == 2) {
    source.offset_alias = binding.offset_binding;
  }
  return source;
}

Expression RewriteLambdaArrayFunction(const GoogleSqlAstNode& array_node,
                                      const GoogleSqlAstNode& lambda,
                                      bool as_filter) {
  Expression array_expr = VisitExpression(array_node);
  // A NULL array argument propagates: UNNEST over it yields no rows, which
  // would otherwise collapse to an empty (non-NULL) result array.
  Expression array_for_null_check = array_expr;
  LambdaBindingResult binding = BindLambdaBody(lambda);
  auto inner = std::make_shared<SelectStatement>(
      as_filter
          ? std::vector<NamedExpression>{NamedExpression(
                std::string(""),
                ColumnValueExp(ColumnName("", binding.element_binding)))}
          : std::vector<NamedExpression>{NamedExpression(
                std::string(""), std::move(binding.body))},
      std::vector<std::string>{},
      as_filter ? binding.body : Expression{});
  inner->SetSources({MakeLambdaSource(std::move(array_expr), binding)});
  inner->MarkComplex();
  auto query_expression =
      std::make_shared<QueryExpression>(std::move(inner), nullptr, false, false);
  query_expression->SetArrayResult(true);
  query_expression->SetArrayElementSqlType(
      InferArrayElementSqlType(lambda.children.back().get() == nullptr
                                   ? lambda
                                   : *lambda.children.back()));
  Expression array_result(query_expression);
  return CaseExpressionExp(
      {{UnaryExpressionExp(std::move(array_for_null_check),
                           UnaryOperation::kIsNull),
        ConstantValueExp(Value())}},
      std::move(array_result));
}

// ARRAY_INCLUDES(array, e -> pred) asks whether any element satisfies the
// predicate: three-valued over a NULL array (NULL), otherwise TRUE when the
// existential subquery finds a row and FALSE when it does not.  An empty
// array yields FALSE because UNNEST produces no rows to satisfy it.
Expression RewriteLambdaIncludes(const GoogleSqlAstNode& array_node,
                                 const GoogleSqlAstNode& lambda) {
  Expression array_expr = VisitExpression(array_node);
  // The NULL-array guard and the UNNEST source share one evaluation of the
  // array argument; keep a handle for both.
  Expression array_for_null_check = array_expr;
  LambdaBindingResult binding = BindLambdaBody(lambda);
  auto inner = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(
          std::string(""),
          ColumnValueExp(ColumnName("", binding.element_binding)))},
      std::vector<std::string>{}, std::move(binding.body));
  inner->SetSources({MakeLambdaSource(std::move(array_expr), binding)});
  inner->MarkComplex();
  auto exists = std::make_shared<QueryExpression>(
      std::move(inner), nullptr, true, false);
  return CaseExpressionExp(
      {{UnaryExpressionExp(std::move(array_for_null_check),
                           UnaryOperation::kIsNull),
        ConstantValueExp(Value())},
       {Expression(std::move(exists)), ConstantValueExp(Value(int64_t{1}))}},
      ConstantValueExp(Value(int64_t{0})));
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
  if (name == "ucase") {
    name = "upper";
  }
  if (name == "lcase") {
    name = "lower";
  }
  if (name == "array_transform" || name == "array_filter" ||
      name == "array_includes") {
    // Higher-order array functions take a trailing lambda argument; desugar
    // the lambda into an ARRAY(SELECT ... FROM UNNEST ...) rewrite (or the
    // existential CASE shape for ARRAY_INCLUDES).
    const GoogleSqlAstNode* array_arg = nullptr;
    const GoogleSqlAstNode* lambda = nullptr;
    for (size_t i = 1; i < node.children.size(); ++i) {
      const GoogleSqlAstNode& child = *node.children[i];
      if (child.kind == "Location") { continue; }
      if (child.kind == "Lambda" && lambda == nullptr) {
        lambda = &child;
        continue;
      }
      if (array_arg == nullptr) { array_arg = &child; }
    }
    if (lambda != nullptr && array_arg != nullptr) {
      if (name == "array_includes") {
        return RewriteLambdaIncludes(*array_arg, *lambda);
      }
      return RewriteLambdaArrayFunction(*array_arg, *lambda,
                                        name == "array_filter");
    }
  }
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
    } else if (name == "left") {
      name = "byte_left";
    } else if (name == "right") {
      name = "byte_right";
    } else if (name == "regexp_instr") {
      name = "byte_regexp_instr";
    } else if (name == "regexp_extract_all") {
      name = "byte_regexp_extract_all";
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
    const bool is_bit =
        name == "bit_and" || name == "bit_or" || name == "bit_xor";
    const bool is_approx_top =
        name == "approx_top_count" || name == "approx_top_sum";
    const size_t approx_arity = name == "approx_top_count" ? 2 : 3;
    // STRING_AGG accepts 1 or 2 arguments and is exempt from the strict
    // single-argument aggregate arity check.
    const bool arity_checked =
        !is_bit && !is_approx_top && name != "string_agg";
    const bool arity_ok =
        !arity_checked || (name == "count" && !arguments.empty()) ||
        arguments.size() == 1 || (is_bit && arguments.size() == 2) ||
        (is_approx_top && arguments.size() == approx_arity);
    if (!arity_ok) {
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
    // ANY_VALUE may legally return any non-NULL group value; MIN provides
    // that with deterministic streaming semantics.
    if (name == "any_value") {
      type = AggregationType::kMin;
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
    if (name == "array_concat_agg") {
      type = AggregationType::kArrayConcatAgg;
    }
    if (name == "elementwise_sum") {
      type = AggregationType::kElementwiseSum;
    }
    if (name == "elementwise_avg") {
      type = AggregationType::kElementwiseAvg;
    }
    if (name == "approx_top_count") {
      type = AggregationType::kApproxTopCount;
    }
    if (name == "approx_top_sum") {
      type = AggregationType::kApproxTopSum;
    }
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
    } else if (name == "percentile_cont") {
      extended = AggregationType::kPercentileCont;
      min_arity = max_arity = 2;
    } else if (name == "approx_count_distinct") {
      extended = AggregationType::kApproxCountDistinct;
      min_arity = max_arity = 1;
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
  if (!UdfRegistry().empty() &&
      UdfRegistry().find(name) != UdfRegistry().end()) {
    if (Expression expanded = ExpandUdfCall(name, std::move(arguments))) {
      return expanded;
    }
  }
  return FunctionCallExp(name, std::move(arguments));
}

// Expands a call to a registered SQL function (CREATE TEMP FUNCTION /
// CREATE TEMP AGGREGATE FUNCTION) by visiting the stored body AST with the
// call arguments substituted for the parameters. Aggregate definitions whose
// expanded body has no aggregate are wrapped in a COUNT(*)-gated CASE so
// they still evaluate once per group (one row over empty input).
Expression ExpandUdfCall(std::string name, std::vector<Expression> arguments) {
  auto& registry = UdfRegistry();
  const auto found = registry.find(name);
  if (found == registry.end()) {
    return nullptr;
  }
  SqlUdf& udf = found->second;
  if (arguments.size() != udf.parameters.size()) {
    throw std::runtime_error("Function call arity mismatch: " + udf.name);
  }
  if (t_udf_frames.size() >= 32) {
    throw std::runtime_error("SQL function recursion limit exceeded: " +
                             udf.name);
  }
  std::vector<Expression> args = std::move(arguments);
  const bool bind_arguments =
      !udf.is_aggregate && udf.simple_body &&
      std::any_of(udf.parameter_counts.begin(), udf.parameter_counts.end(),
                  [](size_t count) { return count > 1; });
  if (bind_arguments) {
    // GoogleSQL evaluates each SQL function argument exactly once per call
    // even when the body references the parameter repeatedly. Bind the
    // arguments as columns of a one-row derived table and select the body
    // from it, leaving parameter identifiers untouched.
    t_udf_bound_masks.push_back({});
    Expression result;
    try {
      std::vector<NamedExpression> inner_projections;
      inner_projections.reserve(args.size());
      for (size_t i = 0; i < args.size(); ++i) {
        inner_projections.emplace_back(udf.parameters[i].first, args[i]);
      }
      auto inner = std::make_shared<SelectStatement>(
          std::move(inner_projections), std::vector<std::string>{},
          Expression{});
      SelectSource source;
      source.join_type = JoinType::kCross;
      source.query = std::move(inner);
      source.alias = "__udf_args";
      auto outer = std::make_shared<SelectStatement>(
          std::vector<NamedExpression>{
              NamedExpression(std::string(""), VisitExpression(*udf.body))},
          std::vector<std::string>{}, Expression{});
      outer->SetSources({std::move(source)});
      outer->MarkComplex();
      result = QueryExpressionExp(std::move(outer));
    } catch (...) {
      t_udf_bound_masks.pop_back();
      throw;
    }
    t_udf_bound_masks.pop_back();
    return result;
  }
  const UdfExpansionFrame frame{&udf, &args, t_udf_bound_masks.size()};
  t_udf_frames.push_back(frame);
  Expression result;
  try {
    result = VisitExpression(*udf.body);
  } catch (...) {
    t_udf_frames.pop_back();
    throw;
  }
  t_udf_frames.pop_back();
  if (udf.is_aggregate && !relational_detail::ContainsAggregate(result)) {
    // NOT AGGREGATE arguments and aggregate-free bodies are group-level
    // expressions: force the grouped path so an empty input still yields
    // one group whose value is the body expression.
    Expression count_star =
        AggregateExpressionExp(AggregationType::kCount, ColumnValueExp("*"));
    Expression condition = BinaryExpressionExp(
        std::move(count_star), BinaryOperation::kGreaterThanEquals,
        ConstantValueExp(Value(int64_t{0})));
    result =
        CaseExpressionExp({{std::move(condition), std::move(result)}}, nullptr);
  }
  return result;
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
        if (child->kind != "TypeParameterList") {
          continue;
        }
        std::string params;
        for (const auto& param : child->children) {
          if (param->kind == "IntLiteral" || param->kind == "Identifier") {
            if (!params.empty()) {
              params += ", ";
            }
            params += param->kind == "IntLiteral" ? param->detail
                                                  : UpperCopy(param->detail);
          }
        }
        if (!params.empty()) {
          base += "(" + params + ")";
        }
        break;
      }
    }
    if (!base.empty()) {
      return base;
    }
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
  if (node.kind == "StructType") {
    // Render STRUCT<T1, T2> / STRUCT<name T, ...> so runtime casts can walk
    // the declared field list.
    std::string fields;
    for (const auto& child : node.children) {
      if (child->kind != "StructField") { continue; }
      std::string name_part;
      if (const auto* id = child->Child("Identifier")) {
        name_part = UpperCopy(Identifier(*id));
      }
      std::string type_part;
      for (const auto& field_child : child->children) {
        if (field_child->kind == "Identifier") { continue; }
        const std::string nested = SqlTypeFromAst(*field_child);
        if (!nested.empty()) {
          type_part = nested;
          break;
        }
      }
      if (type_part.empty()) { continue; }
      if (!fields.empty()) { fields += ", "; }
      fields += name_part.empty() ? type_part : name_part + " " + type_part;
    }
    return "STRUCT<" + fields + ">";
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
  if (node.kind == "JSONLiteral") {
    return "JSON";
  }
  if (node.kind == "NumericLiteral") {
    return "NUMERIC";
  }
  if (node.kind == "BigNumericLiteral") {
    return "BIGNUMERIC";
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
  if (node.kind == "StructConstructorWithParens" ||
      node.kind == "StructConstructorWithKeyword") {
    // Struct elements flatten one level inside UNNEST; the declared tag
    // tells the runtime to expand member columns.
    return "STRUCT";
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

thread_local std::unordered_map<std::string, NamedWindowParts> t_named_windows;

void CollectFromBoundNames(
    const GoogleSqlAstNode& node,  // NOLINT(misc-no-recursion)
    std::unordered_set<std::string>* names) {
  if (node.kind == "TablePathExpression") {
    std::string alias = Alias(node);
    if (!alias.empty()) {
      names->insert(Lower(alias));
    }
    if (const GoogleSqlAstNode* unnest = node.Child("UnnestExpression")) {
      for (const auto& child : unnest->children) {
        if (child->kind == "ExpressionWithOptAlias") {
          std::string unnest_alias = Alias(*child);
          if (!unnest_alias.empty()) {
            names->insert(Lower(unnest_alias));
          }
          break;
        }
      }
      for (const auto& child : unnest->children) {
        if (child->kind.find("Offset") != std::string::npos) {
          if (const GoogleSqlAstNode* offset_alias = child->Child("Alias")) {
            if (offset_alias->Child("Identifier") != nullptr) {
              names->insert(
                  Lower(Identifier(*offset_alias->Child("Identifier"))));
            }
          }
        }
      }
      if (node.Child("WithOffset") != nullptr) {
        if (const GoogleSqlAstNode* offset_alias =
                node.Child("WithOffset")->Child("Alias")) {
          if (offset_alias->Child("Identifier") != nullptr) {
            names->insert(
                Lower(Identifier(*offset_alias->Child("Identifier"))));
          } else {
            names->insert("offset");
          }
        }
      }
    }
    return;
  }
  if (node.kind == "TableSubquery") {
    if (const GoogleSqlAstNode* query = node.Child("Query")) {
      const GoogleSqlAstNode* select =
          (query->kind == "Select") ? query : query->Child("Select");
      if (select != nullptr) {
        if (const GoogleSqlAstNode* list = select->Child("SelectList")) {
          for (const GoogleSqlAstNode* column :
               list->Children("SelectColumn")) {
            std::string name = Alias(*column);
            if (!name.empty()) {
              names->insert(Lower(name));
              continue;
            }
            for (const auto& child : column->children) {
              if (child->kind == "PathExpression" &&
                  child->children.size() == 1) {
                names->insert(Lower(Identifier(*child->children.back())));
                break;
              }
              if (child->kind != "Location" && child->kind != "Alias") {
                break;
              }
            }
          }
        }
      }
      for (const auto& child : query->children) {
        CollectFromBoundNames(*child, names);
      }
      if (select != nullptr && select != query) {
        for (const auto& child : select->children) {
          if (child->kind == "FromClause") {
            for (const auto& from_child : child->children) {
              CollectFromBoundNames(*from_child, names);
            }
          }
        }
      }
    }
    return;
  }
  for (const auto& child : node.children) {
    CollectFromBoundNames(*child, names);
  }
}

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
  if (text.size() < 10) {
    return norm_ts;
  }
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
  double s_val = 0;
  int matched = sscanf(base_time.c_str(), "%d-%d-%d %d:%d:%lf", &Y, &M, &D, &h,
                       &m, &s_val);
  if (matched < 3) {
    matched = sscanf(base_time.c_str(), "%d-%d-%d", &Y, &M, &D);
  }
  if (matched < 3) {
    return norm_ts;
  }
  if (is_leap_sec) {
    m += 1;
    s_val = 0.0;
  }
  if (!has_explicit_tz) {
    total_offset_mins =
        ParseTimeZoneOffset(GetDefaultTimeZone(), Y, M, D, h, m,
                            static_cast<int>(s_val), -8 * 3600) /
        60;
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
             utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday, utc.tm_hour,
             utc.tm_min, utc.tm_sec, frac_str.c_str());
  } else {
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d+00",
             utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday, utc.tm_hour,
             utc.tm_min, utc.tm_sec);
  }
  return std::string(buf);
}

// Splits a JSON object text (as produced by the eager struct constructors
// below) into its top-level key/value pairs, preserving order.
bool SplitJsonObjectFields(
    const std::string& json,
    std::vector<std::pair<std::string, std::string>>* fields) {
  if (json.size() < 2 || json.front() != '{' || json.back() != '}') {
    return false;
  }
  const std::string body = json.substr(1, json.size() - 2);
  int depth = 0;
  bool in_string = false;
  char quote = '\0';
  std::string current;
  auto flush = [&]() {
    const size_t colon = current.find(':');
    if (colon == std::string::npos) {
      return;
    }
    std::string key = current.substr(0, colon);
    if (key.size() >= 2 && key.front() == '"' && key.back() == '"') {
      key = key.substr(1, key.size() - 2);
    }
    fields->emplace_back(std::move(key), current.substr(colon + 1));
  };
  for (size_t i = 0; i < body.size(); ++i) {
    const char c = body[i];
    if (in_string) {
      current.push_back(c);
      if (c == '\\' && i + 1 < body.size()) {
        current.push_back(body[++i]);
        continue;
      }
      if (c == quote) { in_string = false; }
      continue;
    }
    if (c == '"' || c == '\'') {
      in_string = true;
      quote = c;
      current.push_back(c);
      continue;
    }
    if (c == '{' || c == '[') { ++depth; }
    if (c == '}' || c == ']') { --depth; }
    if (c == ',' && depth == 0) {
      flush();
      current.clear();
      continue;
    }
    current.push_back(c);
  }
  if (!current.empty()) { flush(); }
  return true;
}

// An array of struct literals declares field names once (first element or
// STRUCT<...> type); positional siblings like `(2.0, 2.0)` encode generic
// "f1..fN" keys.  Rename those to the leading element's field names (or to
// explicitly declared names when provided) so bare field references resolve
// uniformly across every row.
void AlignAnonymousStructFieldNames(
    std::vector<Expression>* elements,
    const std::vector<std::string>* declared = nullptr) {
  if (elements->size() < 2 && declared == nullptr) {
    return;
  }
  auto encoded_struct = [](const Expression& e) -> std::string {
    if (!e || e->Type() != TypeTag::kConstantValue) {
      return {};
    }
    const Value& value = e->AsConstantValue().GetValue();
    if (value.type != ValueType::kVarChar) {
      return {};
    }
    return std::string(value.value.varchar_value);
  };
  std::vector<std::pair<std::string, std::string>> head_fields;
  if (declared != nullptr) {
    for (const std::string& name : *declared) {
      head_fields.emplace_back(name, "");
    }
  } else if (!elements->empty()) {
    const std::string head = encoded_struct((*elements)[0]);
    if (head.empty()) {
      return;
    }
    if (!SplitJsonObjectFields(head, &head_fields)) {
      return;
    }
    // The head must carry explicit names: anonymous fN keys have nothing to
    // propagate.
    for (size_t i = 0; i < head_fields.size(); ++i) {
      if (head_fields[i].first == "f" + std::to_string(i + 1)) {
        return;
      }
    }
  } else {
    return;
  }
  const size_t first_candidate =
      declared != nullptr ? 0 : 1;
  for (size_t idx = first_candidate; idx < elements->size(); ++idx) {
    const std::string text = encoded_struct((*elements)[idx]);
    if (text.empty()) {
      continue;
    }
    std::vector<std::pair<std::string, std::string>> fields;
    if (!SplitJsonObjectFields(text, &fields) ||
        fields.size() != head_fields.size()) {
      continue;
    }
    bool anonymous = true;
    for (size_t i = 0; i < fields.size(); ++i) {
      if (fields[i].first != "f" + std::to_string(i + 1)) {
        anonymous = false;
        break;
      }
    }
    if (!anonymous) {
      continue;
    }
    std::string rebuilt = "{";
    for (size_t i = 0; i < fields.size(); ++i) {
      if (i > 0) {
        rebuilt += ",";
      }
      rebuilt += "\"" + head_fields[i].first + "\":" + fields[i].second;
    }
    rebuilt += "}";
    (*elements)[idx] = ConstantValueExp(Value(std::move(rebuilt)));
  }
}

Expression VisitExpression(
    const GoogleSqlAstNode&
        node) {  // NOLINT(misc-no-recursion) // Recursive AST descent by
                 // design; stack overflow guarded via ExpressionDepthGuard.
  const ExpressionDepthGuard depth_guard;
  if (node.kind == "PathExpression") {
    std::string path_name = Path(node);
    if (!t_udf_frames.empty()) {
      if (Expression substituted = SubstituteUdfParameter(path_name)) {
        return substituted;
      }
    }
    if (path_name.find('.') == std::string::npos) {
      const auto& constants = SessionConstantExpressions();
      const auto found = constants.find(Lower(path_name));
      if (found != constants.end()) {
        return found->second;
      }
    }
    if (HasSessionConstant(path_name)) {
      return ConstantValueExp(Value(GetSessionConstant(path_name)));
    }
    return ColumnValueExp(path_name);
  }
  if (node.kind == "Star") {
    return ColumnValueExp("*");
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
  if (node.kind == "JSONLiteral" || node.kind == "NumericLiteral" ||
      node.kind == "BigNumericLiteral") {
    // JSON literals keep their verbatim text (the JSON type maps to the
    // engine's string cell representation); NUMERIC / BIGNUMERIC literals
    // evaluate as IEEE doubles, matching the double-typed statistical
    // goldens exercised by the compliance corpus.
    const GoogleSqlAstNode* literal = node.Child("StringLiteral");
    if (literal == nullptr) {
      throw std::runtime_error("GoogleSQL AST: malformed " + node.kind);
    }
    std::string text = DecodeString(*literal);
    if (node.kind == "JSONLiteral") {
      return ConstantValueExp(Value(std::move(text)));
    }
    errno = 0;
    char* parse_end = nullptr;
    const double parsed = std::strtod(text.c_str(), &parse_end);
    if (parse_end == text.c_str() || *parse_end != '\0') {
      throw std::runtime_error("GoogleSQL AST: malformed numeric literal " +
                               text);
    }
    return ConstantValueExp(Value(parsed));
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
    return ConstantValueExp(Value(NormalizeTimestampText(text)));
  }

  if (node.kind == "NullLiteral") {
    return ConstantValueExp(Value());
  }
  // INSERT ... VALUES (30, DEFAULT): tables built by this engine carry no
  // column defaults, so DEFAULT resolves to NULL.
  if (node.kind == "DefaultLiteral") {
    return ConstantValueExp(Value());
  }
  if (node.kind == "BooleanLiteral") {
    const std::string upper_literal = UpperCopy(node.detail);
    return ConstantValueExp(Value(upper_literal == "TRUE"));
  }
  if (node.kind == "ArrayConstructor") {
    std::string element_type;
    // Field names declared on ARRAY<STRUCT<name T, ...>>: parenthesized
    // struct elements carry no StructType of their own, so their eager JSON
    // encoding uses generic fN keys until renamed here.
    std::vector<std::string> struct_field_names;
    std::vector<Expression> elements;
    // Establish the element kind before visiting values so a mixed typed
    // array such as [TIMESTAMP '...', '...'] coerces later string literals.
    for (const auto& child : node.children) {
      if (IsAstTrivia(child->kind) || IsArrayTypeNode(child->kind)) {
        continue;
      }
      element_type = InferArrayElementSqlType(*child);
      if (!element_type.empty()) {
        break;
      }
    }
    for (const auto& child : node.children) {
      if (IsAstTrivia(child->kind)) {
        continue;
      }
      if (IsArrayTypeNode(child->kind)) {
        for (const auto& nested : child->children) {
          for (const GoogleSqlAstNode* field : nested->Children("StructField")) {
            if (const GoogleSqlAstNode* id = field->Child("Identifier")) {
              struct_field_names.push_back(Identifier(*id));
            }
          }
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
        if (!element_type.empty()) {
          break;
        }
      }
    }
    if (element_type.empty()) {
      element_type = "INT64";
    }
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
    AlignAnonymousStructFieldNames(
        &elements,
        struct_field_names.size() >= 2 ? &struct_field_names : nullptr);
    return ArrayExpressionExp(std::move(elements), std::move(element_type));
  }
  // `a << b` / `a >> b`: the dump interleaves a Location node between the
  // operands.
  if (node.kind == "BitwiseShiftExpression") {
    std::vector<const GoogleSqlAstNode*> operands;
    for (const auto& child : node.children) {
      if (child->kind != "Location") {
        operands.push_back(child.get());
      }
    }
    if (operands.size() != 2) {
      throw std::runtime_error("GoogleSQL AST: bit shift arity");
    }
    return BinaryExpressionExp(VisitExpression(*operands[0]),
                               BinaryOp(node.detail),
                               VisitExpression(*operands[1]));
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
    // Bitwise operators have no BinaryOperation tag: desugar into function
    // calls so both the interpreter and plan executor can evaluate them.
    if (node.detail == "&" || node.detail == "|" || node.detail == "^" ||
        node.detail == "<<" || node.detail == ">>") {
      static const std::unordered_map<std::string, std::string> kBitFns = {
          {"&", "__bit_and"}, {"|", "__bit_or"}, {"^", "__bit_xor"},
          {"<<", "__shift_left"}, {">>", "__shift_right"}};
      const std::string fn = kBitFns.at(std::string(node.detail));
      return FunctionCallExp(
          fn, {std::move(left), VisitExpression(*node.children[1])});
    }
    Expression right = VisitExpression(*node.children[1]);
    return BinaryExpressionExp(std::move(left), BinaryOp(node.detail),
                               std::move(right));
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
    // `x IS [NOT] UNKNOWN` arrives as a UnaryExpression (unlike IS NULL /
    // IS TRUE, which the parser shapes as BinaryExpression); UNKNOWN is a
    // NULL predicate, so map it to the corresponding null test.
    if (node.detail == "IS UNKNOWN") {
      return UnaryExpressionExp(VisitExpression(*node.children[0]),
                                UnaryOperation::kIsNull);
    }
    if (node.detail == "IS NOT UNKNOWN") {
      return UnaryExpressionExp(VisitExpression(*node.children[0]),
                                UnaryOperation::kIsNotNull);
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
      if (child->kind == "Location") {
        continue;
      }
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
      if (child->kind == "Location") {
        continue;
      }
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
      if (!base && child->kind != "Location") {
        base = VisitExpression(*child);
      }
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
    if (fn.find("safe") == std::string::npos &&
        (accessor.find("SAFE_") == 0 ||
         accessor.find("_SAFE") != std::string::npos)) {
      fn += "_safe";
    }
    return FunctionCallExp(fn, {std::move(base), VisitExpression(*index_node)});
  }

  if (node.kind == "BitwiseShiftExpression") {
    // `expr << n` / `expr >> n`: children are [expr, Location, n].
    if (node.children.size() < 3) {
      throw std::runtime_error("GoogleSQL AST: malformed shift expression");
    }
    const bool left_shift = node.detail == "<<";
    return FunctionCallExp(
        left_shift ? "__shift_left" : "__shift_right",
        {VisitExpression(*node.children[0]),
         VisitExpression(*node.children[2])});
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
      Expression cond = BinaryExpressionExp(
          value_expr, BinaryOperation::kEquals, std::move(when_val));
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
      if (child->kind != "UnnestExpression") {
        continue;
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
      Expression result = FunctionCallExp(
          "__quantified__", {std::move(test), std::move(array),
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
    const bool safe_cast_target =
        node.detail.find("return_null_on_error=true") != std::string::npos;
    // Integer literals beyond INT64_MAX are UINT64-typed in GoogleSQL;
    // narrowing them to any int width raises out_of_range instead of
    // truncating.
    if (node.children[0]->kind == "IntLiteral") {
      const std::string& digits = node.children[0]->detail;
      uint64_t magnitude = 0;
      if (!digits.empty() &&
          std::from_chars(digits.data(), digits.data() + digits.size(),
                          magnitude)
                  .ec == std::errc() &&
          magnitude >
              static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        std::string target = SqlTypeFromAst(*node.children[1]);
        if (target.empty()) { target = "INT64"; }
        // Keep UINT64 literals above INT64_MAX in the engine's signed
        // bit-pattern representation.  The cast expression and result
        // formatter already interpret this representation as UINT64; only
        // narrowing casts must reject it.
        if (UpperCopy(target) == "UINT64" &&
            (digits.empty() || digits.front() != '-')) {
          return ConstantValueExp(Value(static_cast<int64_t>(magnitude)));
        }
        if (safe_cast_target) { return ConstantValueExp(Value()); }
        throw std::runtime_error(UpperCopy(target) + " out of range: " +
                                 digits);
      }
    }
    // BYTES -> STRING must carry valid UTF-8; GoogleSQL raises on invalid
    // sequences rather than re-encoding them.
    {
      const std::string upper_cast_type =
          UpperCopy(SqlTypeFromAst(*node.children[1]));
      if ((upper_cast_type == "STRING" || upper_cast_type == "VARCHAR") &&
          IsBytesAstNode(*node.children[0])) {
        const std::string bytes = DecodeBytes(*node.children[0]);
        if (!IsValidUtf8Text(bytes)) {
          if (safe_cast_target) { return ConstantValueExp(Value()); }
          throw std::runtime_error(
              "Cannot cast bytes with invalid UTF-8 to STRING");
        }
      }
    }
    ValidateStructCastCoercibility(node, safe_cast_target);
    Expression child = VisitExpression(*node.children[0]);
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
    // A unary-minus integer literal is still a signed value.  UINT64 uses
    // the signed bit-pattern only after a valid non-negative value has been
    // cast, so do not let CAST(-1 AS UINT64) enter that representation.
    if (upper_type == "UINT64" && node.children[0]->kind == "UnaryExpression" &&
        node.children[0]->detail == "-") {
      const GoogleSqlAstNode* literal = node.children[0]->Child("IntLiteral");
      if (literal != nullptr && literal->detail != "0") {
        if (safe_cast_target) { return ConstantValueExp(Value()); }
        throw std::runtime_error("UINT64 out of range: -" + literal->detail);
      }
    }
    // Enum-typed casts stay as runtime CAST expressions: the runtime
    // resolves registry members to their names, rejects unknown values for
    // closed (proto2) enums, keeps unknown in-range values for open (proto3)
    // enums, and raises on out-of-range ordinals. Folding here would leak
    // one literal's result through the SQL template cache to siblings.
    // CAST(<boolean literal> AS STRING) stringifies as the SQL literal; the
    // engine's INT64 boolean encoding would lose that distinction.
    if ((upper_type == "STRING" || upper_type == "VARCHAR") &&
        node.children[0]->kind == "BooleanLiteral") {
      return ConstantValueExp(
          Value(UpperCopy(node.children[0]->detail) == "TRUE" ? "true"
                                                              : "false"));
    }

    // Reading an integer back out of an enum-valued string is handled at
    // runtime: CastValue resolves registry member names to their ordinals
    // and applies the target width validation.
    const bool safe =
        node.detail.find("return_null_on_error=true") != std::string::npos;
    return CastExpressionExp(std::move(child), std::move(type_name), safe);
  }

  if (node.kind == "NewConstructor") {
    return BuildNewConstructor(node);
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
      Expression deferred;  // set when eager evaluation failed
    };
    std::vector<StructFieldJson> fields;
    bool any_ci = false;
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
                // Nested STRUCT values are represented as JSON text in the
                // evaluator, but must remain structural members of the
                // enclosing object rather than becoming a quoted STRING.
                field.is_string =
                    !(arg_node->kind == "StructConstructorWithKeyword" ||
                      arg_node->kind == "StructConstructorWithParens" ||
                      arg_node->kind == "StructConstructorWithType");
              } else if (v.type == ValueType::kInt64) {
                field.text = std::to_string(v.value.int_value);
              } else if (v.type == ValueType::kDouble) {
                field.text = FormatDoubleShortest(v.value.double_value);
              } else {
                field.text = v.AsString();
              }
            }
          } catch (...) {
            // Aggregates and subqueries cannot be evaluated at visit time;
            // defer the field to runtime via the __struct_json__ path.
            field.deferred = val_expr;
          }
        }
      }
      fields.push_back(std::move(field));
      ++arg_idx;
    }
    const bool any_deferred = std::any_of(
        fields.begin(), fields.end(),
        [](const StructFieldJson& f) { return static_cast<bool>(f.deferred); });
    if (any_deferred) {
      // Runtime struct construction: alternating name / value / is-string
      // flag arguments, resolved by EvaluateFunction per row or per group.
      std::vector<Expression> args;
      args.reserve(fields.size() * 3);
      for (auto& f : fields) {
        args.push_back(ConstantValueExp(Value(std::move(f.name))));
        if (f.deferred) {
          args.push_back(std::move(f.deferred));
        } else {
          args.push_back(ConstantValueExp(Value(std::move(f.text))));
        }
        args.push_back(ConstantValueExp(
            Value(f.is_string && !f.deferred ? int64_t{1} : int64_t{0})));
      }
      return FunctionCallExp("__struct_json__", std::move(args));
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
      if (!first) {
        json_out += ",";
      }
      first = false;
      std::string escaped_fname;
      for (char c : field.name) {
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
    // comparison chains; subquery forms reuse the QueryExpression machinery
    // with the comparison operator and quantifier attached.
    Expression lhs;
    std::string quantifier;
    const GoogleSqlAstNode* list_node = nullptr;
    const GoogleSqlAstNode* collection = nullptr;
    const GoogleSqlAstNode* query_node = nullptr;
    for (const auto& child : node.children) {
      if (child->kind == "Location") {
        continue;
      }
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
        return FunctionCallExp(
            "__quantified__",
            {std::move(lhs), std::move(arr),
             ConstantValueExp(Value(std::move(op_text))),
             ConstantValueExp(Value(std::move(quantifier_text)))});
      }
      return QueryExpressionExp(VisitQuery(*query_node), lhs, false, false, op,
                                mode);
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
      if (child->kind == "Location") {
        continue;
      }
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
    const bool negated =
        UpperCopy(node.detail).find("NOT") != std::string::npos;
    if (any_op != nullptr && query_node != nullptr) {
      // `lhs [NOT] LIKE ANY/ALL (subquery)`: per-row LIKE / NOT LIKE under
      // three-valued ANY/ALL combination.
      const bool is_any = UpperCopy(any_op->detail) == "ANY" ||
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

  if (node.kind == "DotGeneralizedField" && node.children.size() >= 2) {
    // proto extension access: value.(pkg.Ext.field).  Lowered to a runtime
    // lookup of the bracketed extension key inside the TEXT payload.
    Expression base = VisitExpression(*node.children[0]);
    std::string extension_path = Path(*node.children[node.children.size() - 1]);
    return FunctionCallExp(
        "__get_extension",
        {std::move(base), ConstantValueExp(Value(std::move(extension_path)))});
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
  // FROM sources are resolved outside the alias scope of their own query
  // block, so the innermost bound-name mask is suspended while descending
  // into them.
  struct SuspendInnermostMask {
    bool active = false;
    std::unordered_set<std::string> restored;
    SuspendInnermostMask() {
      active = !t_udf_frames.empty() && !t_udf_bound_masks.empty();
      if (active) {
        restored = std::move(t_udf_bound_masks.back());
        t_udf_bound_masks.pop_back();
      }
    }
    ~SuspendInnermostMask() {
      if (active) {
        t_udf_bound_masks.push_back(std::move(restored));
      }
    }
  } suspend_mask;
  SelectSource source;
  source.join_type = join_type;
  source.join_condition = std::move(join_condition);
  source.alias = Alias(node);
  // WITH OFFSET applies to both explicit UNNEST operators and implicit
  // unnests written as qualified field paths (`t.arr elem WITH OFFSET off`).
  auto capture_offset_alias = [&]() {
    const GoogleSqlAstNode* with_offset = node.Child("WithOffset");
    if (with_offset == nullptr) {
      with_offset = node.Child("WithOffsetClause");
    }
    if (with_offset != nullptr) {
      if (const GoogleSqlAstNode* alias = with_offset->Child("Alias")) {
        if (alias->Child("Identifier") != nullptr) {
          source.offset_alias = Identifier(*alias->Child("Identifier"));
        } else {
          source.offset_alias = "offset";
        }
      } else {
        source.offset_alias = "offset";
      }
      return;
    }
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
  };
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
      capture_offset_alias();
      if (source.alias.empty()) {
        source.alias = "unnest";
      }
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
    if (dotted.find('.') != std::string::npos && dotted.back() != '.') {
      source.unnest = VisitExpression(*path);
      if (source.alias.empty()) {
        source.alias = dotted.substr(dotted.rfind('.') + 1);
      }
      capture_offset_alias();
      return source;
    }
    source.table = dotted;
    if (source.alias.empty()) {
      source.alias = source.table;
    }
    // TEMP views expand as their stored defining query; the reference alias
    // stays the name used at the reference site.
    const auto& views = ViewRegistry();
    const auto found_view = views.find(Lower(dotted));
    if (found_view != views.end()) {
      if (ViewExpansionDepth() >= 16) {
        throw std::runtime_error("view expansion too deep: " + dotted);
      }
      ++ViewExpansionDepth();
      SelectSource view_source;
      try {
        view_source.query = VisitQuery(*found_view->second);
        view_source.alias =
            source.alias.empty() ? Lower(dotted) : source.alias;
      } catch (...) {
        --ViewExpansionDepth();
        throw;
      }
      --ViewExpansionDepth();
      return view_source;
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
  if (node.detail == "COMMA") {
    type = JoinType::kCross;
  } else if (node.detail == "LEFT") {
    type = JoinType::kLeft;
  } else if (node.detail == "RIGHT") {
    type = JoinType::kRight;
  } else if (node.detail == "FULL") {
    type = JoinType::kFull;
  }
  Expression join_expression;
  std::vector<std::string> using_columns;
  if (on != nullptr && !on->children.empty()) {
    join_expression = VisitExpression(*on->children[0]);
  } else if (using_clause != nullptr) {
    // USING(col, ...) carries no OnClause child; it is an equality join over
    // the shared columns. Dropping it silently would turn the statement into
    // a condition-less join.  The names also ride on the right-hand source so
    // execution can coalesce bare references and star expansion.
    for (const GoogleSqlAstNode* column :
         using_clause->Children("Identifier")) {
      using_columns.push_back(Identifier(*column));
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
  const size_t right_source_index = sources->size();
  AppendSources(*operands[1], type, std::move(join_expression), sources);
  if (!using_columns.empty() && right_source_index < sources->size()) {
    (*sources)[right_source_index].using_columns = std::move(using_columns);
  }
}

std::shared_ptr<SelectStatement> VisitQuery(
    const GoogleSqlAstNode& query) {  // NOLINT(misc-no-recursion) // Recursive
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
        bool union_distinct = false;
        bool union_by_name = false;
        SetOperationKind default_operation = SetOperationKind::kUnionAll;
      const std::string set_detail = UpperCopy(set_op->detail);
        if (set_detail.find("INTERSECT") != std::string::npos) {
          default_operation =
              set_detail.find("ALL") != std::string::npos
                  ? SetOperationKind::kIntersectAll
                  : SetOperationKind::kIntersect;
        } else if (set_detail.find("EXCEPT") != std::string::npos) {
          default_operation =
              set_detail.find("ALL") != std::string::npos
                  ? SetOperationKind::kExceptAll
                  : SetOperationKind::kExcept;
        } else if (set_detail.find("DISTINCT") != std::string::npos) {
          default_operation = SetOperationKind::kUnion;
        }
        if (const GoogleSqlAstNode* metadata_list =
                set_op->Child("SetOperationMetadataList")) {
          for (const GoogleSqlAstNode* metadata :
               metadata_list->Children("SetOperationMetadata")) {
            const GoogleSqlAstNode* mode =
                metadata->Child("SetOperationAllOrDistinct");
            if (mode != nullptr &&
                UpperCopy(mode->detail).find("DISTINCT") != std::string::npos) {
              union_distinct = true;
            }
            if (metadata->Child("SetOperationColumnMatchMode") != nullptr) {
              union_by_name = true;
            }
          }
        }
        if (set_detail.find("DISTINCT") != std::string::npos) {
          union_distinct = true;
        }
        if (union_distinct) {
          first_stmt->MarkUnionDistinct(union_by_name);
        }
        for (size_t i = 1; i < operands.size(); ++i) {
          first_stmt->AddSetOperation(default_operation, VisitQuery(*operands[i]));
        }
        // A set operation is represented by the first operand in the
        // statement tree, but query-level LIMIT/OFFSET and WITH clauses hang
        // off the wrapper.  Preserve them here so execution sees the same
        // scope and applies bounds after concatenating the branches.
        if (const GoogleSqlAstNode* limit_offset =
                query.Child("LimitOffset")) {
          if (const GoogleSqlAstNode* limit_node =
                  limit_offset->Child("Limit")) {
            for (const auto& child : limit_node->children) {
              if (child->kind == "IntLiteral") {
                first_stmt->SetLimit(
                    static_cast<size_t>(ParseUnsignedLiteral(*child)));
                break;
              }
            }
          }
          for (const auto& child : limit_offset->children) {
            if (child->kind == "IntLiteral") {
              first_stmt->SetOffset(ParseUnsignedLiteral(*child));
            }
          }
        }
        if (const GoogleSqlAstNode* order = query.Child("OrderBy")) {
          std::vector<SelectStatement::OrderByTerm> order_by;
          for (const GoogleSqlAstNode* term :
               order->Children("OrderingExpression")) {
            WindowOrderTerm parsed = ParseOrderingTerm(term);
            if (parsed.expression) {
              order_by.push_back({std::move(parsed.expression),
                                  parsed.ascending, parsed.nulls_first});
            }
          }
          first_stmt->SetOrderBy(std::move(order_by));
        }
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
            if (name == nullptr || nested == nullptr) {
              continue;
            }
            const std::string cte_name = Identifier(*name);
            if (recursive) {
              first_stmt->AddRecursiveWithQuery(cte_name,
                                                VisitQuery(*nested));
            } else {
              first_stmt->AddWithQuery(cte_name, VisitQuery(*nested));
            }
          }
        }
        return first_stmt;
      }
    }
    // A parenthesized query can carry its own WITH clause while the outer
    // query wrapper carries another one.  The parser represents that shape
    // as Query -> WithClause, Query (without a Select child directly on the
    // outer node).  Visit the nested query and attach the outer CTE scope.
    for (const auto& child : query.children) {
      if (child->kind != "Query") {
        continue;
      }
      auto nested = VisitQuery(*child);
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
          const GoogleSqlAstNode* body = aliased->Child("Query");
          if (name == nullptr || body == nullptr) {
            continue;
          }
          const std::string cte_name = Identifier(*name);
          if (recursive) {
            nested->AddRecursiveWithQuery(cte_name, VisitQuery(*body));
          } else {
            nested->AddWithQuery(cte_name, VisitQuery(*body));
          }
        }
      }
      return nested;
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
      // `v AS (w ORDER BY z)`: inherit from the referenced definition first,
      // then overlay whatever this specification declares itself.
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
      t_named_windows[Identifier(*name_node)] = std::move(parts);
    }
  }

  // UDF parameter substitution scoping: names explicitly bound by this
  // query block's FROM clause shadow parameters of enclosing expansions.
  std::unordered_set<std::string> bound_names;
  if (!t_udf_frames.empty()) {
    if (const GoogleSqlAstNode* from_clause = select->Child("FromClause")) {
      for (const auto& child : from_clause->children) {
        CollectFromBoundNames(*child, &bound_names);
      }
    }
  }
  t_udf_bound_masks.push_back(std::move(bound_names));
  struct BoundMaskScope {
    ~BoundMaskScope() {
      if (!t_udf_bound_masks.empty()) {
        t_udf_bound_masks.pop_back();
      }
    }
  } bound_mask_scope;

  std::vector<NamedExpression> projections;
  std::vector<const GoogleSqlAstNode*> projection_nodes;
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
    projection_nodes.push_back(expression_node);
  }

  const std::string upper_select_detail = UpperCopy(select->detail);
  const GoogleSqlAstNode* select_as = select->Child("SelectAs");
  const GoogleSqlAstNode* as_struct = select->Child("AsStruct");
  const bool is_as_struct =
      (select_as != nullptr &&
       UpperCopy(select_as->detail).find("STRUCT") != std::string::npos) ||
      as_struct != nullptr;
  // SELECT AS VALUE yields the bare value as the sole column; only named
  // proto/struct targets are folded into a field-list literal.
  const bool as_value =
      select_as != nullptr &&
      UpperCopy(select_as->detail).find("VALUE") != std::string::npos;
  // SELECT AS <proto>: validate projected fields against the registry so
  // required-field violations and invalid enum values fail at prepare time.
  if (!is_as_struct && !as_value && select_as != nullptr) {
    if (const GoogleSqlAstNode* as_path =
            select_as->Child("PathExpression")) {
      std::vector<std::pair<std::string, const GoogleSqlAstNode*>> named;
      const size_t count = std::min(projections.size(), projection_nodes.size());
      named.reserve(count);
      for (size_t i = 0; i < count; ++i) {
        named.emplace_back(projections[i].name, projection_nodes[i]);
      }
      ValidateSelectAsProjections(Path(*as_path), named, &projections);
    }
  }
  if (!is_as_struct && !as_value &&
      (select_as != nullptr ||
       (upper_select_detail.find("AS_MODE=") != std::string::npos &&
        upper_select_detail.find("AS_MODE=VALUE") == std::string::npos))) {
    // SELECT AS <proto type>: fold the projections into a single proto
    // TEXT-format payload through the runtime constructor so non-constant
    // arguments (subqueries, column refs, temporal values) format per row.
    std::string type_name;
    if (select_as != nullptr) {
      for (const auto& child : select_as->children) {
        if (child->kind == "PathExpression" || child->kind == "SimpleType") {
          std::string candidate = child->kind == "PathExpression"
                                      ? Path(*child)
                                      : SqlTypeFromAst(*child);
          for (char& c : candidate) {
            if (c == '`') {
              c = ' ';
            }
          }
          std::string collapsed;
          for (const char c : candidate) {
            if (c != ' ' ||
                (!collapsed.empty() && collapsed.back() != ' ')) {
              collapsed.push_back(c);
            }
          }
          if (!collapsed.empty()) {
            type_name = collapsed;
            break;
          }
        }
      }
    }
    if (type_name.empty()) {
      const size_t mode = upper_select_detail.find("AS_MODE=");
      if (mode != std::string::npos) {
        size_t end = mode + 8;
        while (end < select->detail.size() &&
               select->detail[end] != ',' && select->detail[end] != ' ') {
          ++end;
        }
        type_name = select->detail.substr(mode + 8, end - mode - 8);
      } else if (UpperCopy(select_as != nullptr
                               ? select_as->detail
                               : std::string())
                     .size() > 5) {
        // SelectAs detail carries the raw target text after "type=".
        const std::string& detail = select_as->detail;
        const size_t eq = detail.find("path=");
        if (eq != std::string::npos) {
          size_t end = eq + 5;
          while (end < detail.size() && detail[end] != ',' &&
                 detail[end] != ' ') {
            ++end;
          }
          type_name = detail.substr(eq + 5, end - eq - 5);
        }
      }
    }
    if (!type_name.empty()) {
      std::vector<Expression> args;
      args.emplace_back(ConstantValueExp(Value(std::move(type_name))));
      for (size_t i = 0; i < projections.size() &&
                         i < projection_nodes.size();
           ++i) {
        std::string fname =
            projections[i].name.empty()
                ? ("f" + std::to_string(i + 1))
                : projections[i].name;
        const GoogleSqlAstNode& value_node = *projection_nodes[i];
        if (value_node.kind == "BooleanLiteral") {
          const std::string upper_literal = UpperCopy(value_node.detail);
          args.emplace_back(ConstantValueExp(
              Value(upper_literal == "TRUE" ? std::string("true")
                                            : std::string("false"))));
        } else {
          args.push_back(projections[i].expression);
        }
        args.emplace_back(ConstantValueExp(Value(std::move(fname))));
      }
      projections = {NamedExpression("", FunctionCallExp("__proto_new",
                                                         std::move(args)))};
    }
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
      // GoogleSQL: an unsigned integer ORDER BY item sorts by the
      // SELECT-list ordinal, not by the constant itself.
      if (parsed.expression->Type() == TypeTag::kConstantValue) {
        const Value& ordinal_value =
            parsed.expression->AsConstantValue().GetValue();
        if (ordinal_value.type == ValueType::kInt64 &&
            ordinal_value.value.int_value >= 0) {
          const size_t ordinal =
              static_cast<size_t>(ordinal_value.value.int_value);
          if (ordinal >= 1 && ordinal <= projections.size() &&
              projections[ordinal - 1].expression) {
            order_by.push_back({projections[ordinal - 1].expression,
                                parsed.ascending, parsed.nulls_first});
            continue;
          }
          throw std::runtime_error(
              "GoogleSQL AST: ORDER BY ordinal out of range");
        }
      }
      order_by.push_back(
          {std::move(parsed.expression), parsed.ascending, parsed.nulls_first});
    }
  }

  std::optional<size_t> limit;
  size_t offset = 0;
  // GoogleSQL raises when LIMIT/OFFSET is negative or NULL; the dump shapes
  // those operands as IntLiteral, UnaryExpression(-), or NullLiteral.
  auto validate_limit_operand = [](const GoogleSqlAstNode& operand,
                                   std::string_view clause) {
    if (operand.kind == "NullLiteral") {
      throw std::runtime_error(std::string(clause) +
                               " must not be NULL");
    }
    if (operand.kind == "UnaryExpression" && operand.detail == "-" &&
        !operand.children.empty() &&
        operand.children.front()->kind == "IntLiteral") {
      throw std::runtime_error(std::string(clause) +
                               " must be non-negative");
    }
  };
  if (const GoogleSqlAstNode* limit_offset = query.Child("LimitOffset")) {
    if (const GoogleSqlAstNode* limit_node = limit_offset->Child("Limit")) {
      for (const auto& child : limit_node->children) {
        if (child->kind == "Location" || child->kind == "Hint") { continue; }
        validate_limit_operand(*child, "LIMIT");
        if (child->kind == "IntLiteral") {
          limit = static_cast<size_t>(ParseUnsignedLiteral(*child));
        }
      }
    }
    for (const auto& child : limit_offset->children) {
      if (child->kind == "Limit" || child->kind == "Location" ||
          child->kind == "Hint") {
        continue;
      }
      validate_limit_operand(*child, "OFFSET");
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
    for (const GoogleSqlAstNode* item : group->Children("GroupingItem")) {
      if (item->children.empty()) {
        continue;
      }
      const GoogleSqlAstNode& term = *item->children[0];
      if (term.kind == "IntLiteral") {
        // GoogleSQL: integer GROUP BY items are SELECT-list ordinals.
        const size_t ordinal = static_cast<size_t>(ParseUnsignedLiteral(term));
        if (ordinal >= 1 && ordinal <= statement->SelectList().size() &&
            statement->SelectList()[ordinal - 1].expression) {
          expressions.push_back(
              statement->SelectList()[ordinal - 1].expression);
          continue;
        }
        throw std::runtime_error(
            "GoogleSQL AST: GROUP BY ordinal out of range");
      }
      expressions.push_back(VisitExpression(term));
    }
    statement->SetGroupBy(std::move(expressions));
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
        const std::string cte_name = Identifier(*name);
        if (recursive) {
          statement->AddRecursiveWithQuery(cte_name, VisitQuery(*nested));
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
              const auto bounds =
                  depth_modifier->Children("IntOrUnbounded");
              auto bound_value = [&](size_t index, int64_t fallback) {
                if (index >= bounds.size()) {
                  return fallback;
                }
                for (const auto& child : bounds[index]->children) {
                  if (child->kind == "IntLiteral") {
                    return static_cast<int64_t>(ParseUnsignedLiteral(*child));
                  }
                }
                return fallback;
              };
              spec.lower = bound_value(0, 0);
              spec.upper =
                  bound_value(1, std::numeric_limits<int64_t>::max());
              statement->SetRecursiveDepth(cte_name, std::move(spec));
            }
          }
        } else {
          statement->AddWithQuery(cte_name, VisitQuery(*nested));
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
        source.join_type == JoinType::kFull ||
        !source.using_columns.empty() ||
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
  // Proto / user-defined type names arrive as backticked dotted paths
  // (`googlesql_test.Proto3KitchenSink`) or PROTO<...> wrappers; they store
  // through the VARCHAR channel carrying their TEXT-format payload.
  std::string raw_type = Path(*path);
  std::string cleaned;
  for (const char c : raw_type) {
    if (c != '`') {
      cleaned.push_back(c);
    }
  }
  const std::string lower = Lower(cleaned);
  if (lower.rfind("proto<", 0) == 0 || lower.find('.') != std::string::npos) {
    return ValueType::kVarChar;
  }
  const std::string& type = lower;
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

// CREATE [TEMP] FUNCTION / CREATE TEMP AGGREGATE FUNCTION / CREATE TABLE
// FUNCTION: register scalar and aggregate SQL functions for later call-site
// expansion. Table functions are registered as inert entries (their bodies
// produce relations, which expression call sites cannot consume yet); the
// DDL itself succeeds so downstream statements observe the objects.
const GoogleSqlAstNode* FindSqlFunctionBody(const GoogleSqlAstNode& root) {
  for (const auto& child : root.children) {
    if (child->kind == "SqlFunctionBody") {
      for (const auto& body_child : child->children) {
        if (body_child->kind != "Location") {
          return body_child.get();
        }
      }
    }
  }
  return nullptr;
}

std::unique_ptr<Statement> VisitCreateFunction(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* declaration = root.Child("FunctionDeclaration");
  if (declaration == nullptr) {
    throw std::runtime_error("GoogleSQL AST: function declaration missing");
  }
  const GoogleSqlAstNode* path = declaration->Child("PathExpression");
  if (path == nullptr) {
    throw std::runtime_error("GoogleSQL AST: function without name");
  }
  SqlUdf udf;
  udf.name = Lower(Path(*path));
  udf.is_aggregate = root.detail.find("is_aggregate=true") != std::string::npos;
  if (const GoogleSqlAstNode* parameters =
          declaration->Child("FunctionParameters")) {
    for (const GoogleSqlAstNode* parameter :
         parameters->Children("FunctionParameter")) {
      const GoogleSqlAstNode* name_node = parameter->Child("Identifier");
      if (name_node == nullptr) {
        throw std::runtime_error(
            "GoogleSQL AST: function parameter without name");
      }
      udf.parameters.emplace_back(
          Lower(Identifier(*name_node)),
          parameter->detail.find("is_not_aggregate=true") != std::string::npos);
    }
  }
  if (root.kind != "CreateTableFunctionStatement") {
    const GoogleSqlAstNode* body = FindSqlFunctionBody(root);
    if (body != nullptr) {
      auto cloned = CloneAstNode(root);
      udf.root = std::move(cloned);
      udf.body = FindSqlFunctionBody(*udf.root);
      udf.parameter_counts.assign(udf.parameters.size(), 0);
      AnalyzeUdfBody(*udf.body, udf.parameters, &udf.parameter_counts,
                     &udf.simple_body);
      UdfRegistry()[udf.name] = std::move(udf);
    }
  } else {
    UdfRegistry()[udf.name] = std::move(udf);
  }
  std::vector<NamedExpression> projection;
  projection.emplace_back("", ConstantValueExp(Value(int64_t{0})));
  return std::make_unique<SelectStatement>(
      std::move(projection), std::vector<std::string>{}, Expression{});
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
        if (value->kind == "Location" || value->kind == "Hint") {
          continue;
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
  std::vector<NestedDmlItem> nested_items;
  // Extracts "WHERE <expr>" plus an optional ASSERT_ROWS_MODIFIED from a
  // nested DELETE/UPDATE statement node; remaining children are ignored
  // because nested targets are columns, not relations (no alias/RETURNING).
  auto parse_nested_tail =
      [](const GoogleSqlAstNode& node, Expression* predicate,
         int64_t* assert_rows) {
        std::vector<const GoogleSqlAstNode*> candidates;
        for (const auto& child : node.children) {
          if (child->kind == "PathExpression" ||
              child->kind == "UpdateItemList" ||
              child->kind == "Location" || child->kind == "Hint" ||
              child->kind == "AssertRowsModified") {
            continue;
          }
          if (child->kind == "Alias" || child->kind == "ReturningClause" ||
              child->kind == "FromClause") {
            continue;
          }
          candidates.push_back(child.get());
        }
        if (candidates.size() > 1) {
          throw std::runtime_error(
              "GoogleSQL AST: multiple nested WHERE clause candidates");
        }
        if (!candidates.empty()) {
          *predicate = VisitExpression(*candidates.front());
        }
        if (const GoogleSqlAstNode* assert =
                node.Child("AssertRowsModified")) {
          *assert_rows = AssertRowsModifiedValue(*assert);
        }
      };
  for (const GoogleSqlAstNode* item : items->Children("UpdateItem")) {
    const GoogleSqlAstNode* set = item->Child("UpdateSetValue");
    if (set != nullptr && set->children.size() == 2) {
      assignments.emplace_back(ColumnName(Path(*set->children[0])),
                               VisitExpression(*set->children[1]));
      continue;
    }
    // Nested DML: SET (DELETE arr WHERE ...), SET (UPDATE arr SET ... WHERE
    // ...) and SET (INSERT arr VALUES ... / (SELECT ...)).
    if (const GoogleSqlAstNode* nested = item->Child("DeleteStatement")) {
      const GoogleSqlAstNode* target = nested->Child("PathExpression");
      if (target == nullptr) {
        throw std::runtime_error("GoogleSQL AST: nested DELETE without target");
      }
      NestedDmlItem parsed;
      parsed.kind = NestedDmlItem::Kind::kDelete;
      parsed.target_path = Path(*target);
      parse_nested_tail(*nested, &parsed.predicate,
                        &parsed.assert_rows_modified);
      nested_items.push_back(std::move(parsed));
      continue;
    }
    if (const GoogleSqlAstNode* nested = item->Child("UpdateStatement")) {
      const GoogleSqlAstNode* target = nested->Child("PathExpression");
      const GoogleSqlAstNode* inner_items =
          nested->Child("UpdateItemList");
      if (target == nullptr || inner_items == nullptr ||
          inner_items->Children("UpdateItem").size() != 1) {
        throw std::runtime_error(
            "GoogleSQL AST: bad nested UPDATE assignment");
      }
      const GoogleSqlAstNode* inner_set =
          inner_items->Children("UpdateItem").front()->Child("UpdateSetValue");
      if (inner_set == nullptr || inner_set->children.size() != 2) {
        throw std::runtime_error(
            "GoogleSQL AST: bad nested UPDATE assignment");
      }
      NestedDmlItem parsed;
      parsed.kind = NestedDmlItem::Kind::kUpdate;
      parsed.target_path = Path(*target);
      parsed.set_value = VisitExpression(*inner_set->children[1]);
      parse_nested_tail(*nested, &parsed.predicate,
                        &parsed.assert_rows_modified);
      nested_items.push_back(std::move(parsed));
      continue;
    }
    if (const GoogleSqlAstNode* nested = item->Child("InsertStatement")) {
      const GoogleSqlAstNode* target = nested->Child("PathExpression");
      if (target == nullptr) {
        throw std::runtime_error("GoogleSQL AST: nested INSERT without target");
      }
      NestedDmlItem parsed;
      parsed.kind = NestedDmlItem::Kind::kInsert;
      parsed.target_path = Path(*target);
      if (const GoogleSqlAstNode* row_list =
              nested->Child("InsertValuesRowList")) {
        for (const GoogleSqlAstNode* row :
             row_list->Children("InsertValuesRow")) {
          std::vector<Expression> values;
          for (const auto& value : row->children) {
            if (value->kind == "Location" || value->kind == "Hint") {
              continue;
            }
            values.push_back(VisitExpression(*value));
          }
          parsed.insert_values.push_back(std::move(values));
        }
      }
      if (const GoogleSqlAstNode* query = nested->Child("Query")) {
        parsed.insert_query = VisitQuery(*query);
      }
      if (parsed.insert_values.empty() && parsed.insert_query == nullptr) {
        throw std::runtime_error(
            "GoogleSQL AST: nested INSERT without values or query");
      }
      if (const GoogleSqlAstNode* assert =
              nested->Child("AssertRowsModified")) {
        parsed.assert_rows_modified = AssertRowsModifiedValue(*assert);
      }
      nested_items.push_back(std::move(parsed));
      continue;
    }
    throw std::runtime_error("GoogleSQL AST: bad UPDATE assignment");
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
        child->kind == "ReturningClause" || child->kind == "FromClause") {
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
  statement->SetNestedItems(std::move(nested_items));
  if (const GoogleSqlAstNode* alias = root.Child("Alias")) {
    if (const GoogleSqlAstNode* id = alias->Child("Identifier")) {
      statement->SetAlias(Identifier(*id));
    }
  }
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
        child->kind == "ReturningClause" || child->kind == "FromClause") {
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
  if (const GoogleSqlAstNode* alias = root.Child("Alias")) {
    if (const GoogleSqlAstNode* id = alias->Child("Identifier")) {
      statement->SetAlias(Identifier(*id));
    }
  }
  if (const GoogleSqlAstNode* assert = root.Child("AssertRowsModified")) {
    statement->SetAssertRowsModified(AssertRowsModifiedValue(*assert));
  }
  return statement;
}

// ---------------------------------------------------------------------------
// Proto constructor (NEW) and SELECT AS <proto> validation against the
// embedded compliance-proto registry.

std::string ConstructorTypeFullName(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* path_node = nullptr;
  if (const GoogleSqlAstNode* simple = node.Child("SimpleType")) {
    path_node = simple->Child("PathExpression");
  }
  if (path_node == nullptr) {
    path_node = node.Child("PathExpression");
  }
  if (path_node == nullptr) {
    return {};
  }
  return Path(*path_node);
}

bool FieldNameEquals(const std::string& left, const std::string& right) {
  if (left.size() != right.size()) { return false; }
  for (size_t i = 0; i < left.size(); ++i) {
    const char lc = static_cast<char>(std::tolower(static_cast<unsigned char>(left[i])));
    const char rc = static_cast<char>(std::tolower(static_cast<unsigned char>(right[i])));
    if (lc != rc) { return false; }
  }
  return true;
}

const ProtoFieldSchema* FindProtoField(
    const std::vector<ProtoFieldSchema>& fields, const std::string& name) {
  for (const ProtoFieldSchema& field : fields) {
    if (FieldNameEquals(field.name, name)) { return &field; }
  }
  return nullptr;
}

// Rejects literal enum assignments that the registry says are invalid.
void ValidateEnumLiteralValue(const std::string& message_name,
                              const std::string& field_name,
                              const std::string& enum_type_name,
                              const GoogleSqlAstNode& expr) {
  const std::string enum_short_name(ShortTypeName(enum_type_name));
  if (!IsKnownEnum(enum_short_name)) { return; }
  auto reject = [&](const std::string& message) {
    throw std::runtime_error("Could not store value into proto field " +
                             message_name + "." + field_name + ": " +
                             message);
  };
  if (expr.kind == "StringLiteral") {
    const std::string value = DecodeString(expr);
    int64_t ordinal = 0;
    if (!EnumValueForMember(enum_short_name, value, &ordinal)) {
      reject("Out of range cast of string '" + value + "' to enum type " +
             enum_short_name);
    }
    return;
  }
  if (expr.kind == "IntLiteral") {
    const std::string& digits = expr.detail;
    uint64_t magnitude = 0;
    if (std::from_chars(digits.data(), digits.data() + digits.size(),
                        magnitude)
            .ec != std::errc() ||
        magnitude > static_cast<uint64_t>(2147483647LL)) {
      reject("Out of range cast of integer " + digits + " to enum type " +
             enum_short_name);
      return;
    }
    const int64_t ordinal = static_cast<int64_t>(magnitude);
    const std::optional<std::string> member =
        EnumMemberForValue(enum_short_name, ordinal);
    if (!member.has_value() && !EnumIsOpen(enum_short_name)) {
      reject("Out of range cast of integer " + std::to_string(ordinal) +
             " to enum type " + enum_short_name);
    }
  }
}

// Shared checks for one field assignment. Returns true when the assignment
// still needs a runtime guard because its value is not statically known.
bool ValidateProtoFieldAssignment(const std::string& message_name,
                                  const ProtoFieldSchema& field,
                                  const GoogleSqlAstNode& expr) {
  if (expr.kind == "NullLiteral") {
    if (field.required) {
      throw std::runtime_error(
          "Cannot encode a null value in required protocol message field " +
          message_name + "." + field.name);
    }
    return false;
  }
  if (field.is_enum && !field.repeated &&
      (expr.kind == "StringLiteral" || expr.kind == "IntLiteral")) {
    ValidateEnumLiteralValue(message_name, field.name, field.type_name, expr);
    return false;
  }
  if (field.repeated && expr.kind == "ArrayConstructor") {
    for (const auto& element : expr.children) {
      if (element->kind == "ArrayType" || IsAstTrivia(element->kind)) {
        continue;
      }
      if (element->kind == "NullLiteral") {
        throw std::runtime_error(
            "Cannot encode a null value in repeated protocol message field " +
            message_name + "." + field.name);
      }
    }
    return false;
  }
  // Non-constant repeated arrays and non-constant enum values need runtime
  // validation.
  if (field.repeated &&
      (expr.kind == "QueryExpression" || expr.kind == "FunctionCall" ||
       expr.kind == "CallExpression" || expr.kind == "ScalarSubquery" ||
       expr.kind.starts_with("ExpressionSubquery"))) {
    return true;
  }
  if (field.is_enum && !field.repeated) {
    return true;
  }
  return false;
}

void RequireProtoFieldsPresent(const std::string& message_name,
                               const std::vector<ProtoFieldSchema>& fields,
                               const std::set<std::string>& assigned) {
  for (const ProtoFieldSchema& field : fields) {
    if (field.required) {
      bool found = false;
      for (const std::string& name : assigned) {
        if (FieldNameEquals(name, field.name)) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw std::runtime_error(
            "Required protocol message field " + message_name + "." +
            field.name + " is not assigned");
      }
    }
  }
}

Expression BuildNewConstructor(const GoogleSqlAstNode& node) {
  // Registry validation runs at compile time; the actual payload is built
  // at runtime through __proto_new so per-row values (subqueries, column
  // references, TIMESTAMP/DATE conversions, NULLs) format correctly.
  const std::string message_name = ConstructorTypeFullName(node);
  const std::vector<ProtoFieldSchema>* fields =
      message_name.empty() ? nullptr : FindProtoMessageFields(message_name);
  std::set<std::string> assigned;
  for (const auto& child : node.children) {
    if (child->kind != "NewConstructorArg" || child->children.size() < 2) {
      continue;
    }
    const std::string field_name = Identifier(*child->children[1]);
    if (field_name.empty()) {
      continue;
    }
    assigned.insert(field_name);
    if (fields != nullptr) {
      const ProtoFieldSchema* field = FindProtoField(*fields, field_name);
      if (field != nullptr) {
        ValidateProtoFieldAssignment(message_name, *field,
                                     *child->children[0]);
      }
    }
  }
  if (fields != nullptr) {
    RequireProtoFieldsPresent(message_name, *fields, assigned);
  }
  std::string type_name;
  if (const GoogleSqlAstNode* path_node = node.Child("PathExpression")) {
    type_name = Path(*path_node);
  } else if (const GoogleSqlAstNode* simple = node.Child("SimpleType")) {
    if (const GoogleSqlAstNode* inner = simple->Child("PathExpression")) {
      type_name = Path(*inner);
    }
  }
  {
    for (char& c : type_name) {
      if (c == '`') {
        c = ' ';
      }
    }
    // Collapse whitespace left by backtick removal.
    std::string collapsed;
    for (const char c : type_name) {
      if (c != ' ' || (!collapsed.empty() && collapsed.back() != ' ')) {
        collapsed.push_back(c);
      }
    }
    type_name = std::move(collapsed);
  }
  std::vector<Expression> args;
  args.emplace_back(ConstantValueExp(Value(std::move(type_name))));
  for (const auto& child : node.children) {
    if (child->kind != "NewConstructorArg" || child->children.size() < 2) {
      continue;
    }
    const GoogleSqlAstNode& value_node = *child->children[0];
    const GoogleSqlAstNode& name_node = *child->children[1];
    std::string field_name = Identifier(name_node);
    if (field_name.empty()) {
      field_name = Path(name_node);
    }
    // Extension targets arrive as parenthesized paths: emit the bracketed
    // extension key used by TEXT format.
    if (!field_name.empty() && field_name.front() != '[' &&
        field_name.find('.') != std::string::npos) {
      field_name = "[" + field_name + "]";
    }
    if (value_node.kind == "BooleanLiteral") {
      const std::string upper_literal = UpperCopy(value_node.detail);
      args.emplace_back(ConstantValueExp(
          Value(upper_literal == "TRUE" ? std::string("true")
                                        : std::string("false"))));
    } else {
      args.push_back(VisitExpression(value_node));
    }
    args.emplace_back(ConstantValueExp(Value(std::move(field_name))));
  }
  return FunctionCallExp("__proto_new", std::move(args));
}

void ValidateSelectAsProjections(
    const std::string& message_name,
    const std::vector<std::pair<std::string, const GoogleSqlAstNode*>>& named,
    std::vector<NamedExpression>* projections) {
  const std::vector<ProtoFieldSchema>* fields =
      FindProtoMessageFields(message_name);
  if (fields == nullptr) { return; }
  std::set<std::string> assigned;
  for (const auto& [name, expression_node] : named) {
    assigned.insert(name);
    const ProtoFieldSchema* field = FindProtoField(*fields, name);
    if (field == nullptr) { continue; }
    ValidateProtoFieldAssignment(message_name, *field, *expression_node);
  }
  RequireProtoFieldsPresent(message_name, *fields, assigned);
  for (size_t i = 0; i < named.size(); ++i) {
    const ProtoFieldSchema* field = FindProtoField(*fields, named[i].first);
    if (field == nullptr) { continue; }
    if (named[i].second->kind == "NullLiteral" ||
        named[i].second->kind == "StringLiteral" ||
        named[i].second->kind == "IntLiteral") {
      continue;
    }
    if (field->is_enum && !field->repeated) {
      (*projections)[i] = NamedExpression(
          (*projections)[i].name,
          FunctionCallExp("$proto_enum_guard",
                          {VisitExpression(*named[i].second),
                           ConstantValueExp(Value(std::string(field->type_name)))}));
    }
  }
}

}  // namespace

std::unique_ptr<Statement> GoogleSqlAstVisitor::Visit(
    const GoogleSqlAstNode& root) {
  // Hints for other engines (qualified "engine.name") are ignored; an
  // unqualified hint is only meaningful when the engine knows it, and this
  // engine implements none: GoogleSQL rejects unknown default-engine hints
  // instead of silently executing the statement.
  RejectUnsupportedHints(root);
  if (root.kind == "HintedStatement") {
    for (const auto& child : root.children) {
      if (child->kind == "Hint" || child->kind == "Location") { continue; }
      return Visit(*child);
    }
    throw std::runtime_error("GoogleSQL AST: hinted statement without body");
  }
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
          // Fully evaluable constant: pin the value itself so every
          // reference observes one deterministic result (e.g. Rand()).
          SessionConstantExpressions()[Lower(const_name)] =
              ConstantValueExp(Value(std::move(v)));
        } catch (...) {
          if (child->kind == "StringLiteral") {
            const_val = DecodeString(*child);
            SessionConstantExpressions()[Lower(const_name)] =
                ConstantValueExp(Value(std::move(const_val)));
          } else {
            // Subqueries and function calls evaluate per reference through
            // the relational interpreter instead.
            SessionConstantExpressions()[Lower(const_name)] = std::move(expr);
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
  if (root.kind == "CreateFunctionStatement" ||
      root.kind == "CreateTableFunctionStatement") {
    return VisitCreateFunction(root);
  }
  if (root.kind == "CreateViewStatement") {
    // TEMP views: register the defining query for FROM-reference expansion.
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    const GoogleSqlAstNode* query = root.Child("Query");
    if (path == nullptr || query == nullptr) {
      throw std::runtime_error("GoogleSQL AST: bad CREATE VIEW");
    }
    std::shared_ptr<GoogleSqlAstNode> clone = CloneAstNode(*query);
    ViewRegistry()[Lower(Path(*path))] = std::move(clone);
    std::vector<NamedExpression> projection;
    projection.emplace_back("", ConstantValueExp(Value(int64_t{0})));
    return std::make_unique<SelectStatement>(
        std::move(projection), std::vector<std::string>{}, Expression{});
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
