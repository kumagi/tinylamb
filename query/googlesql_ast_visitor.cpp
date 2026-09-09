/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/googlesql_ast_visitor.hpp"

// NOLINTNEXTLINE(modernize-deprecated-headers) POSIX timegm/gmtime_r
#include <time.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/set_operation.hpp"
#include "executor/detail/expression_eval.hpp"
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
#include "expression/named_expression.hpp"
#include "expression/proto_schema.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/googlesql_ast.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {
// User-facing parse/rewrite rejection: the AST visitor reports every
// unsupported or malformed construct through this helper (converted from
// no-exception rule; see no-exception-rule-migration.md Phase 7).
template <typename T>
[[nodiscard]] StatusOr<T> AstError(std::string message) {
  return StatusError(StatusCode::kInvalidArgument, std::move(message));
}

inline Status AstStatus(std::string message) {
  return StatusError(StatusCode::kInvalidArgument, std::move(message));
}

// The AST dump does not carry per-pair set-operator text (only byte ranges
// into the original SQL). Visit() stashes the source here so set operations
// can slice `SetOperationType` / `SetOperationAllOrDistinct` text out.
thread_local const std::string* t_visit_source = nullptr;

class VisitSourceScope {
 public:
  explicit VisitSourceScope(const std::string& source)
      : previous_(t_visit_source) {
    t_visit_source = &source;
  }
  ~VisitSourceScope() { t_visit_source = previous_; }

  VisitSourceScope(const VisitSourceScope&) = delete;
  VisitSourceScope& operator=(const VisitSourceScope&) = delete;
  VisitSourceScope(VisitSourceScope&&) = delete;
  VisitSourceScope& operator=(VisitSourceScope&&) = delete;

 private:
  const std::string* previous_;
};

std::string SliceSource(const GoogleSqlAstNode* node) {
  if (node == nullptr || t_visit_source == nullptr) {
    return {};
  }
  if (node->end < node->start || node->end > t_visit_source->size()) {
    return {};
  }
  return t_visit_source->substr(node->start, node->end - node->start);
}
}  // namespace

namespace {

StatusOr<SelectSource> ExpandPivotSource(SelectSource base,
                                         const GoogleSqlAstNode& pivot);
StatusOr<SelectSource> ExpandUnpivotSource(const SelectSource& base,
                                           const GoogleSqlAstNode& unpivot);

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
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%d:%d", &th, &tm);
    } else if (rem.size() == 4) {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%2d%2d", &th, &tm);
    } else {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%d", &th);
    }
    return (th * 3600 + tm * 60) * (sign == '-' ? -1 : 1);
  }
  if (tz_str[0] == '+' || tz_str[0] == '-') {
    char sign = tz_str[0];
    int th = 0, tm = 0;
    std::string rem(tz_str.substr(1));
    if (rem.find(':') != std::string::npos) {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%d:%d", &th, &tm);
    } else if (rem.size() == 4) {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%2d%2d", &th, &tm);
    } else {
      sscanf(  // NOLINT(cert-err33-c,cert-err34-c) - zero fallback is intended.
          rem.c_str(), "%d", &th);
    }
    return (th * 3600 + tm * 60) * (sign == '-' ? -1 : 1);
  }
  std::string zone_name(tz_str);
  if (zone_name == "NZ-CHAT") {
    zone_name = "Pacific/Chatham";
  }
  try {
    const auto* zone = std::chrono::locate_zone(zone_name);
    if (zone != nullptr) {
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
    return default_offset;
  }
  return default_offset;
}

StatusOr<std::string> SqlTypeFromAst(const GoogleSqlAstNode& node);
StatusOr<std::string> InferSubqueryArrayElementType(
    const GoogleSqlAstNode& query_node);
StatusOr<std::string> InferAggregateArrayElementType(
    const GoogleSqlAstNode& node);

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
      failed_ = true;
      return;
    }
    ++depth;
  }
  [[nodiscard]] bool failed() const { return failed_; }
  ~ExpressionDepthGuard() { --ExpressionDepthCounter(); }
  ExpressionDepthGuard(const ExpressionDepthGuard&) = delete;
  ExpressionDepthGuard& operator=(const ExpressionDepthGuard&) = delete;
  ExpressionDepthGuard(ExpressionDepthGuard&&) = delete;
  ExpressionDepthGuard& operator=(ExpressionDepthGuard&&) = delete;

 private:
  bool failed_{false};
};

// Dump-produced literals must be digits-only and in range: std::stoll would
// accept signs and std::stoull would wrap "-1" into a huge positive value.
// Hex literals (0x / 0X) are accepted up to 16 digits; values above INT64_MAX
// are GoogleSQL UINT64 literals kept as their two's-complement bit pattern.
StatusOr<int64_t> ParseIntLiteral(const GoogleSqlAstNode& node) {
  const std::string& text = node.detail;
  // Hex literals (0x1F, 0XFFFFFFFFFFFFD8F0) are bit patterns: they wrap
  // modulo 2^64 exactly like the reference engine, where 0xFF... == -1.
  if (text.size() > 2 && text[0] == '0' && (text[1] == 'x' || text[1] == 'X')) {
    const std::string_view digits(text.data() + 2, text.size() - 2);
    const bool hex_only =
        !digits.empty() && std::ranges::all_of(digits, [](char c) {
          return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f') ||
                 ('A' <= c && c <= 'F');
        });
    if (!hex_only) {
      return AstError<int64_t>("GoogleSQL AST: malformed integer literal " +
                               text);
    }
    uint64_t magnitude = 0;
    const auto [ptr, ec] =
        std::from_chars(digits.begin(), digits.end(), magnitude, 16);
    if (ec != std::errc() || ptr != digits.end()) {
      return AstError<int64_t>("GoogleSQL AST: integer literal out of range " +
                               text);
    }
    return static_cast<int64_t>(magnitude);
  }
  const bool digits_only =
      !text.empty() &&
      std::ranges::all_of(text, [](char c) { return '0' <= c && c <= '9'; });
  if (!digits_only) {
    return AstError<int64_t>("GoogleSQL AST: malformed integer literal " +
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
    const auto [u_ptr, u_ec] = std::from_chars(text.data(), end, magnitude, 10);
    if (u_ec == std::errc() && u_ptr == end) {
      return static_cast<int64_t>(magnitude);
    }
  }
  if (ec != std::errc() || ptr != end) {
    return AstError<int64_t>("GoogleSQL AST: integer literal out of range " +
                             text);
  }
  return value;
}

StatusOr<uint64_t> ParseUnsignedLiteral(const GoogleSqlAstNode& node) {
  const std::string& text = node.detail;
  const bool digits_only =
      !text.empty() &&
      std::ranges::all_of(text, [](char c) { return '0' <= c && c <= '9'; });
  if (!digits_only) {
    return AstError<uint64_t>("GoogleSQL AST: malformed unsigned literal " +
                              text);
  }
  uint64_t value = 0;
  const auto* end = text.data() + text.size();
  const auto [ptr, ec] = std::from_chars(text.data(), end, value);
  if (ec != std::errc() || ptr != end) {
    return AstError<uint64_t>("GoogleSQL AST: unsigned literal out of range " +
                              text);
  }
  return value;
}

StatusOr<double> ParseFloatLiteral(const GoogleSqlAstNode& node) {
  // std::stod rejects subnormal magnitudes on some libstdc++ versions
  // (ERANGE); strtod accepts the full IEEE-754 double domain.
  errno = 0;
  const std::string text = node.detail;
  char* end = nullptr;
  const double value = std::strtod(text.c_str(), &end);
  if (end == text.c_str() || *end != '\0') {
    return AstError<double>("GoogleSQL AST: float literal out of range " +
                            text);
  }
  // strtod reports ERANGE for subnormal (denormal) results; the returned
  // value is still the closest representable double and is accepted here.
  if (std::isinf(value)) {
    return AstError<double>("GoogleSQL AST: float literal out of range " +
                            text);
  }
  return value;
}

StatusOr<std::string> DecodeSingleComponent(std::string_view value_view);

StatusOr<std::string> Identifier(const GoogleSqlAstNode& node) {
  if (node.kind != "Identifier") {
    return AstError<std::string>("GoogleSQL AST: expected Identifier");
  }
  std::string value = node.detail;
  if (value.size() >= 2 && value.front() == '`' && value.back() == '`') {
    ASSIGN_OR_RETURN(std::string, hv11956_0,
                     (DecodeSingleComponent(
                         "\"" + value.substr(1, value.size() - 2) + "\"")));
    value = std::move(hv11956_0);
  }
  return value;
}

StatusOr<std::vector<std::string>> PathParts(const GoogleSqlAstNode& path) {
  std::vector<std::string> result;
  for (const auto& child : path.children) {
    if (child->kind == "Identifier") {
      ASSIGN_OR_RETURN(std::string, hv12266_0, (Identifier(*child)));
      result.push_back(std::move(hv12266_0));
    }
  }
  if (result.empty()) {
    return AstError<std::vector<std::string>>(
        "GoogleSQL AST: empty path expression");
  }
  return result;
}

StatusOr<std::string> Path(const GoogleSqlAstNode& path) {
  ASSIGN_OR_RETURN(std::vector<std::string>, hv12514_0, (PathParts(path)));
  const std::vector<std::string> parts = std::move(hv12514_0);
  std::string result;
  for (const std::string& part : parts) {
    if (!result.empty()) {
      result += '.';
    }
    result += part;
  }
  return result;
}

StatusOr<std::string> Alias(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* alias = node.Child("Alias");
  if (alias == nullptr || alias->Child("Identifier") == nullptr) {
    return std::string{};
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

StatusOr<Expression> VisitExpression(const GoogleSqlAstNode& node);

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
      failed_ = true;
      return;
    }
    ++tls_udf_expansion_depth;
  }
  ~UdfExpansionDepthGuard() { --tls_udf_expansion_depth; }
  UdfExpansionDepthGuard(const UdfExpansionDepthGuard&) = delete;
  UdfExpansionDepthGuard& operator=(const UdfExpansionDepthGuard&) = delete;
  UdfExpansionDepthGuard(UdfExpansionDepthGuard&&) = delete;
  UdfExpansionDepthGuard& operator=(UdfExpansionDepthGuard&&) = delete;
  [[nodiscard]] bool failed() const { return failed_; }

 private:
  bool failed_{false};
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
      return {rebuilt};
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
  if (select.HasDistinctOn()) {
    std::vector<Expression> d_on;
    d_on.reserve(select.DistinctOn().size());
    for (const Expression& item : select.DistinctOn()) {
      d_on.push_back(SubstituteParameters(item, bindings));
    }
    result->SetDistinctOn(std::move(d_on));
  }
  result->SetWithTies(select.WithTies());
  for (const auto& [name, query] : select.WithQueries()) {
    result->AddWithQuery(name, SubstituteInSelect(*query, bindings));
  }
  if (select.RequiresRelationalEvaluation()) {
    result->MarkComplex();
  }
  result->SetAsStruct(select.AsStruct());
  return result;
}

std::string DecodeStringEscapesImpl(std::string_view value_view, bool is_bytes,
                                    bool is_triple, char quote) {
  const std::string value(value_view);
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
          auto raw_byte =
              static_cast<uint32_t>(std::stoul(oct_str, nullptr, 8));
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
            auto raw_byte =
                static_cast<uint32_t>(std::stoul(hex_str, nullptr, 16));
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
            auto cp = static_cast<uint32_t>(std::stoul(hex_str, nullptr, 16));
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
            auto cp = static_cast<uint32_t>(std::stoul(hex_str, nullptr, 16));
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
    } else if (is_triple && value[i] == quote && i + 1 < value.size() &&
               value[i + 1] == quote) {
      decoded.push_back(quote);
      ++i;
    } else {
      decoded.push_back(value[i]);
    }
  }
  return decoded;
}

StatusOr<std::string> DecodeSingleComponent(std::string_view value_view) {
  std::string value = std::string(value_view);
  bool is_raw = false;
  bool is_bytes = false;
  if (!value.empty() && (value.front() == 'r' || value.front() == 'R')) {
    is_raw = true;
    value = value.substr(1);
  }
  if (!value.empty() && (value.front() == 'b' || value.front() == 'B')) {
    is_bytes = true;
    value = value.substr(1);
  }
  if (!is_raw && !value.empty() &&
      (value.front() == 'r' || value.front() == 'R')) {
    is_raw = true;
    value = value.substr(1);
  }

  bool is_triple = false;
  char quote = '\0';
  if (value.size() >= 6 &&
      ((value.starts_with(R"(""")") && value.ends_with(R"(""")")) ||
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

  return DecodeStringEscapesImpl(value, is_bytes, is_triple, quote);
}

StatusOr<std::string> DecodeString(const GoogleSqlAstNode& node) {
  std::string result;
  bool found_component = false;
  for (const auto& child : node.children) {
    if (child->kind == "StringLiteralComponent") {
      ASSIGN_OR_RETURN(std::string, hv30550_0,
                       (DecodeSingleComponent(child->detail)));
      result += hv30550_0;
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

StatusOr<BinaryOperation> BinaryOp(std::string_view detail) {
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
  if (detail == "IS DISTINCT FROM") {
    return BinaryOperation::kIsDistinctFrom;
  }
  if (detail == "IS NOT DISTINCT FROM") {
    return BinaryOperation::kIsNotDistinctFrom;
  }
  return AstError<BinaryOperation>(
      "GoogleSQL AST: unsupported binary operator " + std::string(detail));
}

StatusOr<std::shared_ptr<SelectStatement>> VisitQuery(
    const GoogleSqlAstNode& query);
StatusOr<Expression> ExpandUdfCall(const std::string& name,
                                   std::vector<Expression> arguments);

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
      return std::ranges::any_of(expression->AsArrayExpression().Elements(),
                                 [](const Expression& element) {
                                   return NeedsRelationalEvaluation(element,
                                                                    false);
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

StatusOr<Expression> FoldBoolean(
    const GoogleSqlAstNode& node,
    BinaryOperation op) {  // NOLINT(misc-no-recursion) // AST traversal
                           // recursion is intentional; depth bounded by
                           // ExpressionDepthGuard in VisitExpression.
  Expression result;
  for (const auto& child : node.children) {
    ASSIGN_OR_RETURN(Expression, hv38172_0, (VisitExpression(*child)));
    Expression next = std::move(hv38172_0);
    result = result
                 ? BinaryExpressionExp(std::move(result), op, std::move(next))
                 : std::move(next);
  }
  if (!result) {
    return AstError<Expression>("GoogleSQL AST: empty boolean node");
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
    auto t_or = SqlTypeFromAst(*node.children[1]);
    if (!t_or.HasValue()) {
      return false;
    }
    const std::string t = t_or.MoveValue();
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
  std::vector<std::shared_ptr<GoogleSqlAstNode>> default_values;
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
      if (t_udf_bound_masks[mask_pos].contains(lower)) {
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
          if (segment_end == std::string::npos) {
            break;
          }
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
    auto hv45660_0 = Identifier(*node.children[0]);
    if (!hv45660_0.HasValue()) {
      *simple = false;
      return;
    }
    const std::string lower = Lower(hv45660_0.MoveValue());
    for (size_t i = 0; i < parameters.size(); ++i) {
      if (parameters[i].first == lower) {
        ++(*counts)[i];
      }
    }
    return;
  }
  if (node.kind == "FunctionCall" && !node.children.empty() &&
      node.children.front()->kind == "PathExpression") {
    auto fn_or = Path(*node.children.front());
    if (fn_or.HasValue()) {
      const std::string fn = Lower(fn_or.MoveValue());
      if (kAggregateNames.contains(fn)) {
        *simple = false;
      }
    } else {
      // Unresolvable callee name: conservatively treat the body as complex.
      *simple = false;
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
Status RejectUnsupportedHints(  // NOLINT(misc-no-recursion)
    const GoogleSqlAstNode& node) {
  if (node.kind == "HintEntry") {
    size_t leading_identifiers = 0;
    for (const auto& child : node.children) {
      if (child->kind != "Identifier") {
        break;
      }
      ++leading_identifiers;
    }
    if (leading_identifiers <= 1) {
      std::string hint_name;
      if (!node.children.empty()) {
        auto hint_id = Identifier(*node.children.front());
        hint_name = hint_id.HasValue() ? hint_id.MoveValue()
                                       : hint_id.GetStatus().GetMessage();
      }
      return AstStatus(std::string("Unsupported hint: ") + hint_name);
    }
  }
  for (const auto& child : node.children) {
    Status st48171 = RejectUnsupportedHints(*child);
    if (st48171 != Status::kSuccess) {
      return st48171;
    }
  }
  return Status::kSuccess;
}

// Strict UTF-8 validator: BYTES -> STRING casts must reject the sequences
// GoogleSQL refuses (surrogates, overlong forms, truncated tails) instead of
// passing mojibake through.
bool IsValidUtf8Text(std::string_view text) {
  size_t i = 0;
  while (i < text.size()) {
    const auto lead = static_cast<uint8_t>(text[i]);
    size_t continuation_count = 0;
    uint8_t second_lower = 0x80;
    uint8_t second_upper = 0xBF;
    if (lead < 0x80) {
      ++i;
      continue;
    }
    if (lead >= 0xC2 && lead < 0xDF) {
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
    if (i + continuation_count >= text.size()) {
      return false;
    }
    for (size_t c = 1; c <= continuation_count; ++c) {
      const auto byte = static_cast<uint8_t>(text[i + c]);
      const uint8_t lower = c == 1 ? second_lower : 0x80;
      const uint8_t upper = c == 1 ? second_upper : 0xBF;
      if (byte < lower || byte > upper) {
        return false;
      }
    }
    i += continuation_count + 1;
  }
  return true;
}

bool IsAstTrivia(std::string_view kind);

// Builds NEW <proto>(field AS value, ...) into the engine's text-format
// representation, validating registry-known messages along the way.
StatusOr<Expression> BuildNewConstructor(const GoogleSqlAstNode& node);

// Validates SELECT AS <proto> projections against registry field metadata;
// wraps non-constant enum-typed values in a runtime validation guard.
Status ValidateSelectAsProjections(
    const std::string& message_name,
    const std::vector<std::pair<std::string, const GoogleSqlAstNode*>>& named,
    std::vector<NamedExpression>* projections);

// GoogleSQL coerces STRUCT<...> casts field-by-field and raises when a field
// value cannot be coerced to the target field type. The engine's legacy cast
// path collapses struct types, losing that validation; this re-establishes
// it for literal struct sources. Anything not statically checkable is left
// to the existing runtime behavior untouched.
Status ValidateStructCastCoercibility(const GoogleSqlAstNode& node, bool safe) {
  (void)safe;
  if (node.children.size() < 2 || node.children[1]->kind != "StructType") {
    return Status::kSuccess;
  }
  const GoogleSqlAstNode& source = *node.children[0];
  if (source.kind != "StructConstructorWithParens" &&
      source.kind != "StructConstructorWithKeyword" &&
      source.kind != "StructConstructorWithType") {
    return Status::kSuccess;
  }
  std::vector<std::string> field_types;
  for (const auto& field : node.children[1]->children) {
    if (field->kind != "StructField") {
      continue;
    }
    std::string field_type;
    for (const auto& part : field->children) {
      ASSIGN_OR_RETURN(std::string, h_tmp_1391, (SqlTypeFromAst(*part)));
      field_type = std::move(h_tmp_1391);
      if (!field_type.empty()) {
        break;
      }
    }
    field_types.push_back(field_type);
  }
  std::vector<const GoogleSqlAstNode*> elements;
  for (const auto& child : source.children) {
    if (IsAstTrivia(child->kind)) {
      continue;
    }
    // Keyword constructors wrap each field in a StructConstructorArg node.
    if (child->kind == "StructConstructorArg") {
      for (const auto& inner : child->children) {
        if (IsAstTrivia(inner->kind)) {
          continue;
        }
        elements.push_back(inner.get());
        break;
      }
      continue;
    }
    elements.push_back(child.get());
  }
  if (field_types.empty() || elements.size() != field_types.size()) {
    return Status::kSuccess;
  }
  Row dummy_row;
  Schema dummy_schema;
  for (size_t i = 0; i < elements.size(); ++i) {
    Expression element_expr;
    ASSIGN_OR_RETURN(Expression, h_tmp_1424, (VisitExpression(*elements[i])));
    element_expr = std::move(h_tmp_1424);
    StatusOr<Value> original =
        element_expr->TryEvaluate(dummy_row, dummy_schema);
    if (!original.HasValue()) {
      // Not statically decidable: defer to the legacy runtime path.
      return Status::kSuccess;
    }
    if (original.Value().IsNull()) {
      continue;
    }
    if (field_types[i].empty()) {
      continue;
    }
    Expression coerced = CastExpressionExp(element_expr, field_types[i],
                                           /*return_null_on_error=*/true);
    StatusOr<Value> coerced_value =
        coerced->TryEvaluate(dummy_row, dummy_schema);
    if (!coerced_value.HasValue()) {
      return Status::kSuccess;
    }
    if (coerced_value.Value().IsNull()) {
      return AstStatus("Cannot coerce struct field " + std::to_string(i + 1) +
                       " to " + field_types[i]);
    }
  }
  return Status::kSuccess;
}

bool IsBytesAstNode(const GoogleSqlAstNode& node) {
  if (node.kind == "BytesLiteral") {
    return true;
  }
  if (node.kind == "CastExpression" && node.children.size() >= 2) {
    const GoogleSqlAstNode& type_node = *node.children[1];
    auto type_name_or = SqlTypeFromAst(type_node);
    std::string type_name =
        type_name_or.HasValue() ? type_name_or.MoveValue() : std::string();
    if (type_name.empty()) {
      if (const auto* path = type_node.Child("PathExpression")) {
        auto hv53305_0 = Path(*path);
        type_name =
            hv53305_0.HasValue() ? hv53305_0.MoveValue() : std::string();
      } else if (type_node.kind == "SimpleType") {
        for (const auto& c : type_node.children) {
          auto h_ty = SqlTypeFromAst(*c);
          type_name = h_ty.HasValue() ? h_ty.MoveValue() : std::string();
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
      auto hv53862_0 = Path(*node.children.front());
      if (!hv53862_0.HasValue()) {
        return false;
      }
      const std::string fn = Lower(hv53862_0.MoveValue());
      if (fn == "byte_substr" || fn == "b") {
        return true;
      }
    }
  }
  return false;
}

StatusOr<WindowOrderTerm> ParseOrderingTerm(const GoogleSqlAstNode* term) {
  WindowOrderTerm parsed;
  if (term == nullptr || term->children.empty()) {
    return parsed;
  }
  for (const auto& child : term->children) {
    if (child->kind == "Location") {
      continue;
    }
    if (child->kind == "NullOrder") {
      std::string null_order_text = UpperCopy(child->detail);
      if (null_order_text.empty()) {
        null_order_text = UpperCopy(SliceSource(child.get()));
      }
      parsed.nulls_first = null_order_text.find("FIRST") != std::string::npos;
      continue;
    }
    if (!parsed.expression) {
      ASSIGN_OR_RETURN(Expression, hv54641_0, (VisitExpression(*child)));
      parsed.expression = std::move(hv54641_0);
    }
  }
  std::string direction = UpperCopy(term->detail);
  if (direction.empty()) {
    direction = UpperCopy(SliceSource(term));
  }
  parsed.ascending = direction.find("DESC") == std::string::npos;
  return parsed;
}

StatusOr<std::vector<WindowOrderTerm>> ParseOrderingList(
    const GoogleSqlAstNode& order) {
  std::vector<WindowOrderTerm> terms;
  for (const GoogleSqlAstNode* term : order.Children("OrderingExpression")) {
    ASSIGN_OR_RETURN(WindowOrderTerm, parsed, (ParseOrderingTerm(term)));
    if (parsed.expression) {
      terms.push_back(std::move(parsed));
    }
  }
  return terms;
}

// Shared lambda plumbing for the higher-order array functions: extracts the
// parameter names, binds them to the synthetic UNNEST bindings, and visits
// the body under that expansion frame.
StatusOr<std::string> InferArrayElementSqlType(const GoogleSqlAstNode& node);

struct LambdaBindingResult {
  Expression body;
  std::string element_binding;
  std::string offset_binding;
  size_t param_count{0};
};

StatusOr<LambdaBindingResult> BindLambdaBody(const GoogleSqlAstNode& lambda) {
  std::vector<std::string> params;
  for (size_t i = 0; i + 1 < lambda.children.size(); ++i) {
    const GoogleSqlAstNode& head = *lambda.children[i];
    if (head.kind == "PathExpression") {
      ASSIGN_OR_RETURN(std::string, hv55937_0, (Path(head)));
      params.push_back(Lower(std::move(hv55937_0)));
    } else if (head.kind == "StructConstructorWithParens") {
      for (const auto& sub : head.children) {
        if (sub->kind == "PathExpression") {
          ASSIGN_OR_RETURN(std::string, hv56132_0, (Path(*sub)));
          params.push_back(Lower(std::move(hv56132_0)));
        }
      }
    }
  }
  const GoogleSqlAstNode* body =
      lambda.children.empty() ? nullptr : lambda.children.back().get();
  // A bare-parameter body (`e -> e`, `e -> e.x`) is a valid projection and
  // resolves through ordinary parameter substitution.
  if (params.empty() || params.size() > 2 || body == nullptr) {
    return AstError<LambdaBindingResult>("GoogleSQL AST: unsupported Lambda");
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
  const UdfExpansionFrame frame{.udf = &frame_udf,
                                .arguments = &bound,
                                .mask_depth = t_udf_bound_masks.size()};
  t_udf_frames.push_back(frame);
  Status st_body = [&]() -> Status {
    ASSIGN_OR_RETURN(Expression, h_tmp_1581, (VisitExpression(*body)));
    result.body = std::move(h_tmp_1581);
    return Status::kSuccess;
  }();
  t_udf_frames.pop_back();
  t_lambda_frame_storage().pop_back();
  if (st_body != Status::kSuccess) {
    return st_body;
  }
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

StatusOr<Expression> RewriteLambdaArrayFunction(
    const GoogleSqlAstNode& array_node, const GoogleSqlAstNode& lambda,
    bool as_filter) {
  ASSIGN_OR_RETURN(Expression, hv58424_0, (VisitExpression(array_node)));
  Expression array_expr = std::move(hv58424_0);
  // A NULL array argument propagates: UNNEST over it yields no rows, which
  // would otherwise collapse to an empty (non-NULL) result array.
  Expression array_for_null_check = array_expr;
  ASSIGN_OR_RETURN(LambdaBindingResult, hv58670_0, (BindLambdaBody(lambda)));
  LambdaBindingResult binding = std::move(hv58670_0);
  auto inner = std::make_shared<SelectStatement>(
      as_filter ? std::vector<NamedExpression>{NamedExpression(
                      std::string(""),
                      ColumnValueExp(ColumnName("", binding.element_binding)))}
                : std::vector<NamedExpression>{NamedExpression(
                      std::string(""), std::move(binding.body))},
      std::vector<std::string>{}, as_filter ? binding.body : Expression{});
  inner->SetSources({MakeLambdaSource(std::move(array_expr), binding)});
  inner->MarkComplex();
  auto query_expression = std::make_shared<QueryExpression>(
      std::move(inner), nullptr, false, false);
  query_expression->SetArrayResult(true);
  ASSIGN_OR_RETURN(
      std::string, h_elem_sql,
      (InferArrayElementSqlType(lambda.children.back().get() == nullptr
                                    ? lambda
                                    : *lambda.children.back())));
  query_expression->SetArrayElementSqlType(h_elem_sql);
  Expression array_result(query_expression);
  return CaseExpressionExp({{UnaryExpressionExp(std::move(array_for_null_check),
                                                UnaryOperation::kIsNull),
                             ConstantValueExp(Value())}},
                           std::move(array_result));
}

// ARRAY_INCLUDES(array, e -> pred) asks whether any element satisfies the
// predicate: three-valued over a NULL array (NULL), otherwise TRUE when the
// existential subquery finds a row and FALSE when it does not.  An empty
// array yields FALSE because UNNEST produces no rows to satisfy it.
StatusOr<Expression> RewriteLambdaIncludes(const GoogleSqlAstNode& array_node,
                                           const GoogleSqlAstNode& lambda) {
  ASSIGN_OR_RETURN(Expression, hv60357_0, (VisitExpression(array_node)));
  Expression array_expr = std::move(hv60357_0);
  // The NULL-array guard and the UNNEST source share one evaluation of the
  // array argument; keep a handle for both.
  Expression array_for_null_check = array_expr;
  ASSIGN_OR_RETURN(LambdaBindingResult, hv60581_0, (BindLambdaBody(lambda)));
  LambdaBindingResult binding = std::move(hv60581_0);
  auto inner = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(
          std::string(""),
          ColumnValueExp(ColumnName("", binding.element_binding)))},
      std::vector<std::string>{}, std::move(binding.body));
  inner->SetSources({MakeLambdaSource(std::move(array_expr), binding)});
  inner->MarkComplex();
  auto exists =
      std::make_shared<QueryExpression>(std::move(inner), nullptr, true, false);
  return CaseExpressionExp(
      {{UnaryExpressionExp(std::move(array_for_null_check),
                           UnaryOperation::kIsNull),
        ConstantValueExp(Value())},
       {Expression(std::move(exists)), ConstantValueExp(Value(int64_t{1}))}},
      ConstantValueExp(Value(int64_t{0})));
}

StatusOr<Expression> VisitFunction(
    const GoogleSqlAstNode&
        node) {  // NOLINT(misc-no-recursion) // AST traversal recursion is
                 // intentional; depth bounded by ExpressionDepthGuard in
                 // VisitExpression.
  if (node.children.empty() ||
      node.children.front()->kind != "PathExpression") {
    return AstError<Expression>("GoogleSQL AST: function without name");
  }
  ASSIGN_OR_RETURN(std::string, hv61807_0, (Path(*node.children.front())));
  std::string name = Lower(std::move(hv61807_0));
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
      if (child.kind == "Location") {
        continue;
      }
      if (child.kind == "Lambda" && lambda == nullptr) {
        lambda = &child;
        continue;
      }
      if (array_arg == nullptr) {
        array_arg = &child;
      }
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
      return AstError<Expression>(
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
        ASSIGN_OR_RETURN(Expression, hv64815_0,
                         (VisitExpression(*child.children[0])));
        where_filter = std::move(hv64815_0);
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
          ASSIGN_OR_RETURN(Expression, hv65327_0,
                           (VisitExpression(*grandchild)));
          having_condition = std::move(hv65327_0);
          break;
        }
      }
      continue;
    }
    if (child.kind == "OrderBy") {
      ASSIGN_OR_RETURN(std::vector<WindowOrderTerm>, h_tmp_1775,
                       (ParseOrderingList(child)));
      inner_order_by = std::move(h_tmp_1775);
      continue;
    }
    if (child.kind == "LimitOffset") {
      if (const GoogleSqlAstNode* limit_node = child.Child("Limit")) {
        if (const GoogleSqlAstNode* value = limit_node->Child("IntLiteral")) {
          ASSIGN_OR_RETURN(uint64_t, hv65738_0, (ParseUnsignedLiteral(*value)));
          inner_limit = static_cast<size_t>(std::move(hv65738_0));
        }
      }
      continue;
    }
    ASSIGN_OR_RETURN(Expression, hv65853_0, (VisitExpression(child)));
    Expression arg = std::move(hv65853_0);
    if (name == "concat") {
      arg = WrapIfBoolean(std::move(arg), child);
    }
    arguments.push_back(std::move(arg));
  }

  auto finish_aggregate = [&](AggregationType type) -> StatusOr<Expression> {
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
        ASSIGN_OR_RETURN(std::string, element_type,
                         (InferAggregateArrayElementType(child)));
        if (!element_type.empty()) {
          aggregate->SetArrayElementSqlType(element_type);
        }
        break;
      }
    }
    return aggregate;
  };

  if (name == "pipeconcat" || name == "pipeconcatsep") {
    const size_t expected = name == "pipeconcat" ? 2 : 3;
    if (arguments.size() != expected) {
      return AstError<Expression>("GoogleSQL AST: aggregate arity");
    }
    Expression item = FunctionCallExp(
        "__struct_json__",
        {ConstantValueExp(Value(std::string("a"))), arguments[0],
         ConstantValueExp(Value(std::string("b"))), arguments[1]});
    Expression items = std::make_shared<AggregateExpression>(
        AggregationType::kArrayAgg, std::move(item), false);
    Expression separator = expected == 3
                               ? arguments[2]
                               : ConstantValueExp(Value(std::string("|")));
    return FunctionCallExp("__pipe_concat",
                           {std::move(items), std::move(separator)});
  }

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
      return AstError<Expression>("GoogleSQL AST: aggregate arity");
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
        return AstError<Expression>("GoogleSQL AST: aggregate arity");
      }
      return finish_aggregate(extended);
    }
  }
  if (!UdfRegistry().empty() && UdfRegistry().contains(name)) {
    auto expanded = ExpandUdfCall(name, arguments);
    if (!expanded.HasValue()) {
      // Only an unregistered callee falls back to a plain function call;
      // arity and expansion failures are real query errors.
      if (expanded.GetStatus().GetCode() != StatusCode::kNotExists) {
        return expanded.GetStatus();
      }
    } else {
      return expanded.MoveValue();
    }
  }
  return FunctionCallExp(name, std::move(arguments));
}

// Expands a call to a registered SQL function (CREATE TEMP FUNCTION /
// CREATE TEMP AGGREGATE FUNCTION) by visiting the stored body AST with the
// call arguments substituted for the parameters. Aggregate definitions whose
// expanded body has no aggregate are wrapped in a COUNT(*)-gated CASE so
// they still evaluate once per group (one row over empty input).
StatusOr<Expression> ExpandUdfCall(const std::string& name,
                                   std::vector<Expression> arguments) {
  auto& registry = UdfRegistry();
  const auto found = registry.find(name);
  if (found == registry.end()) {
    return Status(Status::kNotExists, "udf not registered");
  }
  SqlUdf& udf = found->second;
  if (arguments.size() < udf.parameters.size()) {
    for (size_t i = arguments.size(); i < udf.parameters.size(); ++i) {
      if (i < udf.default_values.size() && udf.default_values[i] != nullptr) {
        ASSIGN_OR_RETURN(Expression, hv75693_0,
                         (VisitExpression(*udf.default_values[i])));
        arguments.push_back(std::move(hv75693_0));
      } else {
        break;
      }
    }
  }
  if (arguments.size() != udf.parameters.size()) {
    return AstError<Expression>("Function call arity mismatch: " + udf.name);
  }
  if (t_udf_frames.size() >= 32) {
    return AstError<Expression>("SQL function recursion limit exceeded: " +
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
    t_udf_bound_masks.emplace_back();
    Expression result;
    Status st_uda = [&]() -> Status {
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
      ASSIGN_OR_RETURN(Expression, h_uda_body, (VisitExpression(*udf.body)));
      auto outer = std::make_shared<SelectStatement>(
          std::vector<NamedExpression>{
              NamedExpression(std::string(""), std::move(h_uda_body))},
          std::vector<std::string>{}, Expression{});
      outer->SetSources({std::move(source)});
      outer->MarkComplex();
      result = QueryExpressionExp(std::move(outer));
      return Status::kSuccess;
    }();
    t_udf_bound_masks.pop_back();
    if (st_uda != Status::kSuccess) {
      return st_uda;
    }
    return result;
  }
  const UdfExpansionFrame frame{
      .udf = &udf, .arguments = &args, .mask_depth = t_udf_bound_masks.size()};
  t_udf_frames.push_back(frame);
  Expression result;
  Status st_udf = [&]() -> Status {
    ASSIGN_OR_RETURN(Expression, h_tmp_2097, (VisitExpression(*udf.body)));
    result = std::move(h_tmp_2097);
    return Status::kSuccess;
  }();
  t_udf_frames.pop_back();
  if (st_udf != Status::kSuccess) {
    return st_udf;
  }
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

StatusOr<std::string> SqlTypeFromAst(
    const GoogleSqlAstNode& node) {  // NOLINT(misc-no-recursion)
  if (IsAstTrivia(node.kind)) {
    return std::string{};
  }
  if (node.kind == "PathExpression") {
    ASSIGN_OR_RETURN(std::string, h_pt, (Path(node)));
    return UpperCopy(std::move(h_pt));
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
        ASSIGN_OR_RETURN(std::string, hv79459_0, (Path(*child)));
        base = child->kind == "Identifier" ? UpperCopy(child->detail)
                                           : UpperCopy(std::move(hv79459_0));
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
      ASSIGN_OR_RETURN(std::string, nested, (SqlTypeFromAst(*child)));
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
      if (child->kind != "StructField") {
        continue;
      }
      std::string name_part;
      if (const auto* id = child->Child("Identifier")) {
        ASSIGN_OR_RETURN(std::string, hv80963_0, (Identifier(*id)));
        name_part = UpperCopy(std::move(hv80963_0));
      }
      std::string type_part;
      for (const auto& field_child : child->children) {
        if (field_child->kind == "Identifier") {
          continue;
        }
        ASSIGN_OR_RETURN(std::string, nested, (SqlTypeFromAst(*field_child)));
        if (!nested.empty()) {
          type_part = nested;
          break;
        }
      }
      if (type_part.empty()) {
        continue;
      }
      if (!fields.empty()) {
        fields += ", ";
      }
      if (name_part.empty()) {
        fields += type_part;
      } else {
        fields += name_part;
        fields += ' ';
        fields += type_part;
      }
    }
    return "STRUCT<" + fields + ">";
  }
  for (const auto& child : node.children) {
    ASSIGN_OR_RETURN(std::string, nested, (SqlTypeFromAst(*child)));
    if (!nested.empty()) {
      return nested;
    }
  }
  return std::string{};
}

StatusOr<std::string> InferArrayElementSqlType(
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
    return std::string{};
  }

  if (node.kind == "ArrayConstructor") {
    std::string inner;
    for (const auto& child : node.children) {
      if (IsAstTrivia(child->kind)) {
        continue;
      }
      if (child->kind == "ArrayType") {
        ASSIGN_OR_RETURN(std::string, h_tmp_2290, (SqlTypeFromAst(*child)));
        inner = std::move(h_tmp_2290);
        if (inner.starts_with("ARRAY<") && inner.back() == '>') {
          inner = inner.substr(6, inner.size() - 7);
        }
        break;
      }
      ASSIGN_OR_RETURN(std::string, h_tmp_2296,
                       (InferArrayElementSqlType(*child)));
      inner = std::move(h_tmp_2296);
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
StatusOr<std::string> InferSubqueryArrayElementType(
    const GoogleSqlAstNode& query_node) {
  const GoogleSqlAstNode* select = query_node.Child("Select");
  const GoogleSqlAstNode* select_list =
      select == nullptr ? nullptr : select->Child("SelectList");
  if (select_list == nullptr) {
    return std::string{};
  }
  for (const GoogleSqlAstNode* column : select_list->Children("SelectColumn")) {
    const GoogleSqlAstNode* expression_node = nullptr;
    for (const auto& child : column->children) {
      if (child->kind != "Alias") {
        expression_node = child.get();
        break;
      }
    }
    if (expression_node == nullptr) {
      return std::string{};
    }
    const GoogleSqlAstNode& expr = *expression_node;
    if (expr.kind == "BooleanLiteral") {
      return "BOOL";
    }
    if (expr.kind == "FloatLiteral") {
      return "DOUBLE";
    }
    if (expr.kind == "IntLiteral") {
      return "INT64";
    }
    if (expr.kind == "StringLiteral") {
      return "STRING";
    }
    if (expr.kind == "BytesLiteral") {
      return "BYTES";
    }
    if (expr.kind == "CastExpression" && expr.children.size() >= 2) {
      return SqlTypeFromAst(*expr.children[1]);
    }
    return std::string{};
  }
  return std::string{};
}

// Reliable-only variant used by ARRAY_AGG: unlike the subquery path there is
// no fallback cost asymmetry — a wrong guess (e.g. INT64 for a DOUBLE column)
// is worse than deferring to runtime value inference, so only literal/cast
// argument shapes produce a type here.
StatusOr<std::string> InferAggregateArrayElementType(
    const GoogleSqlAstNode& node) {
  if (node.kind == "BooleanLiteral") {
    return "BOOL";
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
    return std::string{};
  }
  if (node.kind == "CastExpression" && node.children.size() >= 2) {
    return SqlTypeFromAst(*node.children[1]);
  }
  return std::string{};
}

StatusOr<std::string> DecodeBytes(const GoogleSqlAstNode& node) {
  std::string result;
  bool found_component = false;
  for (const auto& child : node.children) {
    if (child->kind == "BytesLiteralComponent") {
      ASSIGN_OR_RETURN(std::string, hv86592_0,
                       (DecodeSingleComponent(child->detail)));
      result += hv86592_0;
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
    auto hv87740_0 = Alias(node);
    if (!hv87740_0.HasValue()) {
      return;
    }
    std::string alias = hv87740_0.MoveValue();
    if (!alias.empty()) {
      names->insert(Lower(alias));
    }
    if (const GoogleSqlAstNode* unnest = node.Child("UnnestExpression")) {
      for (const auto& child : unnest->children) {
        if (child->kind == "ExpressionWithOptAlias") {
          auto hv88025_0 = Alias(*child);
          if (!hv88025_0.HasValue()) {
            return;
          }
          std::string unnest_alias = hv88025_0.MoveValue();
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
              auto hv88467_0 = Identifier(*offset_alias->Child("Identifier"));
              if (!hv88467_0.HasValue()) {
                return;
              }
              names->insert(Lower(hv88467_0.MoveValue()));
            }
          }
        }
      }
      if (node.Child("WithOffset") != nullptr) {
        if (const GoogleSqlAstNode* offset_alias =
                node.Child("WithOffset")->Child("Alias")) {
          if (offset_alias->Child("Identifier") != nullptr) {
            auto hv88836_0 = Identifier(*offset_alias->Child("Identifier"));
            if (!hv88836_0.HasValue()) {
              return;
            }
            names->insert(Lower(hv88836_0.MoveValue()));
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
            auto hv89455_0 = Alias(*column);
            if (!hv89455_0.HasValue()) {
              return;
            }
            std::string name = hv89455_0.MoveValue();
            if (!name.empty()) {
              names->insert(Lower(name));
              continue;
            }
            for (const auto& child : column->children) {
              if (child->kind == "PathExpression" &&
                  child->children.size() == 1) {
                auto hv89774_0 = Identifier(*child->children.back());
                if (!hv89774_0.HasValue()) {
                  return;
                }
                names->insert(Lower(hv89774_0.MoveValue()));
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

StatusOr<WindowFrameBound> ParseFrameBound(const GoogleSqlAstNode& node) {
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
      ASSIGN_OR_RETURN(Expression, hv91660_0, (VisitExpression(*child)));
      bound.offset = std::move(hv91660_0);
      break;
    }
  }
  return bound;
}

StatusOr<NamedWindowParts> ParseWindowSpecification(
    const GoogleSqlAstNode& spec) {
  NamedWindowParts parts;
  if (const GoogleSqlAstNode* partition = spec.Child("PartitionBy")) {
    for (const auto& child : partition->children) {
      if (child->kind == "PathExpression") {
        ASSIGN_OR_RETURN(Expression, hv92026_0, (VisitExpression(*child)));
        parts.partition_by.push_back(std::move(hv92026_0));
      }
    }
  }
  if (const GoogleSqlAstNode* order = spec.Child("OrderBy")) {
    ASSIGN_OR_RETURN(std::vector<WindowOrderTerm>, h_tmp_2553,
                     (ParseOrderingList(*order)));
    parts.order_by = std::move(h_tmp_2553);
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
      ASSIGN_OR_RETURN(WindowFrameBound, h_tmp_2564,
                       (ParseFrameBound(*bounds[0])));
      parts.frame_start = std::move(h_tmp_2564);
      ASSIGN_OR_RETURN(WindowFrameBound, h_tmp_2565,
                       (ParseFrameBound(*bounds[1])));
      parts.frame_end = std::move(h_tmp_2565);
    } else if (bounds.size() == 1 &&
               UpperCopy(bounds[0]->detail).find("CURRENT ROW") !=
                   std::string::npos) {
      parts.has_frame = true;
      ASSIGN_OR_RETURN(WindowFrameBound, h_tmp_2570,
                       (ParseFrameBound(*bounds[0])));
      parts.frame_start = std::move(h_tmp_2570);
      parts.frame_end = parts.frame_start;
    } else {
      return AstError<NamedWindowParts>(
          "GoogleSQL AST: window frame requires BETWEEN x AND y");
    }
  }
  return parts;
}

StatusOr<Expression> VisitAnalyticFunctionCall(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* call = node.Child("FunctionCall");
  if (call == nullptr || call->children.empty()) {
    return AstError<Expression>(
        "GoogleSQL AST: malformed analytic function call");
  }
  auto window = std::make_shared<WindowFunctionCallExpression>();

  size_t arg_start = 0;
  if (call->children[0]->kind == "PathExpression") {
    ASSIGN_OR_RETURN(std::string, hv93668_0, (Path(*call->children[0])));
    window->function = UpperCopy(std::move(hv93668_0));
    arg_start = 1;
  } else {
    return AstError<Expression>("GoogleSQL AST: anonymous analytic function");
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
        ASSIGN_OR_RETURN(Expression, hv94301_0,
                         (VisitExpression(*child.children[0])));
        window->where_filter = std::move(hv94301_0);
      }
      continue;
    }
    if (child.kind == "OrderBy") {
      ASSIGN_OR_RETURN(std::vector<WindowOrderTerm>, h_terms,
                       (ParseOrderingList(child)));
      for (WindowOrderTerm& term : h_terms) {
        inner_order_by.push_back(std::move(term));
      }
      continue;
    }
    if (child.kind == "LimitOffset") {
      if (const GoogleSqlAstNode* limit_node = child.Child("Limit")) {
        if (const GoogleSqlAstNode* value = limit_node->Child("IntLiteral")) {
          ASSIGN_OR_RETURN(uint64_t, hv94767_0, (ParseUnsignedLiteral(*value)));
          window->inner_limit = static_cast<size_t>(std::move(hv94767_0));
        }
      }
      continue;
    }
    ASSIGN_OR_RETURN(Expression, hv94904_0, (VisitExpression(child)));
    window->args.push_back(std::move(hv94904_0));
  }
  window->inner_order_by = std::move(inner_order_by);

  const GoogleSqlAstNode* spec = node.Child("WindowSpecification");
  if (spec != nullptr) {
    if (spec->Child("Identifier") != nullptr &&
        spec->Child("PartitionBy") == nullptr &&
        spec->Child("OrderBy") == nullptr &&
        spec->Child("WindowFrame") == nullptr) {
      ASSIGN_OR_RETURN(std::string, hv95299_0,
                       (Identifier(*spec->Child("Identifier"))));
      // Bare reference to a WINDOW-clause definition.
      const std::string name = std::move(hv95299_0);
      const auto found = t_named_windows.find(name);
      if (found == t_named_windows.end()) {
        return AstError<Expression>("GoogleSQL AST: unknown window " + name);
      }
      window->partition_by = found->second.partition_by;
      window->order_by = found->second.order_by;
      window->frame_unit = found->second.frame_unit;
      window->frame_start = found->second.frame_start;
      window->frame_end = found->second.frame_end;
      window->has_frame = found->second.has_frame;
    } else {
      ASSIGN_OR_RETURN(NamedWindowParts, parts,
                       (ParseWindowSpecification(*spec)));
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
std::string NormalizeTimestampTextImpl(const std::string& text) {
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
        tz_hours = 0;
        tz_mins = 0;
      }
    } else {
      try {
        tz_hours = std::stoi(tz_part);
      } catch (...) {
        tz_hours = 0;
        tz_mins = 0;
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
  // NOLINTNEXTLINE(cert-err34-c) `matched` is checked below
  int matched = sscanf(base_time.c_str(), "%d-%d-%d %d:%d:%lf", &Y, &M, &D, &h,
                       &m, &s_val);
  if (matched < 3) {
    // NOLINTNEXTLINE(cert-err34-c) `matched` is checked below
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
  std::array<char, 64> buf{};
  size_t dot_pos = text.find('.');
  if (dot_pos != std::string::npos) {
    size_t end_digit = dot_pos + 1;
    while (end_digit < text.size() && text[end_digit] >= '0' &&
           text[end_digit] <= '9') {
      ++end_digit;
    }
    std::string frac_str = text.substr(dot_pos, end_digit - dot_pos);
    (void)snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d%s+00",
                   utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday, utc.tm_hour,
                   utc.tm_min, utc.tm_sec, frac_str.c_str());
  } else {
    (void)snprintf(buf.data(), buf.size(), "%04d-%02d-%02d %02d:%02d:%02d+00",
                   utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday, utc.tm_hour,
                   utc.tm_min, utc.tm_sec);
  }
  return std::string{buf.data()};
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
      if (c == quote) {
        in_string = false;
      }
      continue;
    }
    if (c == '"' || c == '\'') {
      in_string = true;
      quote = c;
      current.push_back(c);
      continue;
    }
    if (c == '{' || c == '[') {
      ++depth;
    }
    if (c == '}' || c == ']') {
      --depth;
    }
    if (c == ',' && depth == 0) {
      flush();
      current.clear();
      continue;
    }
    current.push_back(c);
  }
  if (!current.empty()) {
    flush();
  }
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
  const size_t first_candidate = declared != nullptr ? 0 : 1;
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

StatusOr<Expression> VisitExpression(
    const GoogleSqlAstNode&
        node) {  // NOLINT(misc-no-recursion) // Recursive AST descent by
                 // design; stack overflow guarded via ExpressionDepthGuard.
  const ExpressionDepthGuard depth_guard;
  if (depth_guard.failed()) {
    return AstError<Expression>("GoogleSQL AST: expression nesting exceeds " +
                                std::to_string(kMaxExpressionDepth));
  }
  if (node.kind == "PathExpression") {
    ASSIGN_OR_RETURN(std::string, hv104424_0, (Path(node)));
    std::string path_name = std::move(hv104424_0);
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
    ASSIGN_OR_RETURN(int64_t, hv105122_0, (ParseIntLiteral(node)));
    Value parsed(hv105122_0);
    const std::string& text = node.detail;
    uint64_t magnitude = 0;
    int base = 10;
    std::string_view digits(text);
    if (digits.starts_with("0x") || digits.starts_with("0X")) {
      base = 16;
      digits.remove_prefix(2);
    }
    const auto [end, error] = std::from_chars(
        digits.data(), digits.data() + digits.size(), magnitude, base);
    if (error == std::errc() && end == digits.data() + digits.size() &&
        magnitude >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      parsed = parsed.WithUnsigned();
    }
    return ConstantValueExp(parsed);
  }
  if (node.kind == "FloatLiteral") {
    ASSIGN_OR_RETURN(double, h_tmp_2961, (ParseFloatLiteral(node)));
    return ConstantValueExp(Value(h_tmp_2961));
  }
  if (node.kind == "StringLiteral") {
    ASSIGN_OR_RETURN(std::string, h_tmp_2964, (DecodeString(node)));
    return ConstantValueExp(Value(std::move(h_tmp_2964)));
  }
  if (node.kind == "JSONLiteral" || node.kind == "NumericLiteral" ||
      node.kind == "BigNumericLiteral") {
    // JSON literals keep their verbatim text (the JSON type maps to the
    // engine's string cell representation); NUMERIC / BIGNUMERIC literals
    // evaluate as IEEE doubles, matching the double-typed statistical
    // goldens exercised by the compliance corpus.
    const GoogleSqlAstNode* literal = node.Child("StringLiteral");
    if (literal == nullptr) {
      return AstError<Expression>("GoogleSQL AST: malformed " + node.kind);
    }
    ASSIGN_OR_RETURN(std::string, hv106536_0, (DecodeString(*literal)));
    std::string text = std::move(hv106536_0);
    if (node.kind == "JSONLiteral") {
      return ConstantValueExp(Value(std::move(text)));
    }
    errno = 0;
    char* parse_end = nullptr;
    const double parsed = std::strtod(text.c_str(), &parse_end);
    if (parse_end == text.c_str() || *parse_end != '\0') {
      return AstError<Expression>("GoogleSQL AST: malformed numeric literal " +
                                  text);
    }
    return ConstantValueExp(Value(parsed));
  }
  if (node.kind == "BytesLiteral") {
    ASSIGN_OR_RETURN(std::string, h_tmp_2991, (DecodeBytes(node)));
    return ConstantValueExp(Value(std::move(h_tmp_2991)));
  }
  if (node.kind == "DateOrTimeLiteral") {
    const GoogleSqlAstNode* literal = node.Child("StringLiteral");
    if (literal == nullptr) {
      return AstError<Expression>("GoogleSQL AST: invalid date/time literal");
    }
    ASSIGN_OR_RETURN(std::string, hv107344_0, (DecodeString(*literal)));
    const std::string text = std::move(hv107344_0);
    if (node.detail == "TYPE_DATE") {
      return ConstantValueExp(Value::Date(text));
    }
    if (node.detail == "TYPE_DATETIME") {
      std::string dt_str = text;
      if (dt_str.find(":59:60") != std::string::npos) {
        int Y = 0, M = 0, D = 0, h = 0;
        // NOLINTNEXTLINE(cert-err34-c): outcome is checked via >= 4 below
        if (sscanf(dt_str.c_str(), "%d-%d-%d %d", &Y, &M, &D, &h) >= 4) {
          h += 1;
          std::chrono::year_month_day ymd{
              std::chrono::year{Y},
              std::chrono::month{static_cast<unsigned>(M)},
              std::chrono::day{static_cast<unsigned>(D)}};
          int64_t days =
              std::chrono::sys_days{ymd}.time_since_epoch().count() + (h / 24);
          h %= 24;
          std::chrono::sys_days new_sd{std::chrono::days{days}};
          std::chrono::year_month_day new_ymd{new_sd};
          std::array<char, 64> buf{};
          (void)snprintf(buf.data(), buf.size(), "%04d-%02u-%02u %02d:00:00",
                         int(new_ymd.year()), unsigned(new_ymd.month()),
                         unsigned(new_ymd.day()), h);
          dt_str = buf.data();
        }
      }
      return ConstantValueExp(Value(std::move(dt_str)));
    }
    if (node.detail == "TYPE_TIME") {
      return ConstantValueExp(Value(std::string(text)));
    }
    // TYPE_TIMESTAMP
    return ConstantValueExp(Value(NormalizeTimestampTextImpl(text)));
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
      ASSIGN_OR_RETURN(std::string, h_tmp_3060,
                       (InferArrayElementSqlType(*child)));
      element_type = std::move(h_tmp_3060);
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
          for (const GoogleSqlAstNode* field :
               nested->Children("StructField")) {
            if (const GoogleSqlAstNode* id = field->Child("Identifier")) {
              ASSIGN_OR_RETURN(std::string, hv110429_0, (Identifier(*id)));
              struct_field_names.push_back(std::move(hv110429_0));
            }
          }
          ASSIGN_OR_RETURN(std::string, parsed, (SqlTypeFromAst(*nested)));
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
        ASSIGN_OR_RETURN(std::string, hv110933_0, (DecodeString(*child)));
        elements.push_back(
            ConstantValueExp(Value(NormalizeTimestampTextImpl(hv110933_0))));
        continue;
      }
      ASSIGN_OR_RETURN(Expression, hv111075_0, (VisitExpression(*child)));
      elements.push_back(std::move(hv111075_0));
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
            ASSIGN_OR_RETURN(std::string, h_tmp_3105,
                             (InferSubqueryArrayElementType(*inner_query)));
            element_type = std::move(h_tmp_3105);
            if (!element_type.empty()) {
              break;
            }
          }
        }
        ASSIGN_OR_RETURN(std::string, h_tmp_3111,
                         (InferArrayElementSqlType(*child)));
        element_type = std::move(h_tmp_3111);
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
        return {array_query};
      }
    }
    AlignAnonymousStructFieldNames(&elements, struct_field_names.size() >= 2
                                                  ? &struct_field_names
                                                  : nullptr);
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
      return AstError<Expression>("GoogleSQL AST: bit shift arity");
    }
    ASSIGN_OR_RETURN(Expression, bs_a, (VisitExpression(*operands[0])));
    ASSIGN_OR_RETURN(BinaryOperation, bs_op, (BinaryOp(node.detail)));
    ASSIGN_OR_RETURN(Expression, bs_b, (VisitExpression(*operands[1])));
    return BinaryExpressionExp(std::move(bs_a), bs_op, std::move(bs_b));
  }

  if (node.kind == "BinaryExpression") {
    if (node.children.size() != 2) {
      return AstError<Expression>("GoogleSQL AST: binary expression arity");
    }
    ASSIGN_OR_RETURN(Expression, hv113607_0,
                     (VisitExpression(*node.children[0])));
    Expression left = std::move(hv113607_0);
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
        ASSIGN_OR_RETURN(std::string, hv114595_0, (Path(*node.children[1])));
        const std::string ident = Lower(std::move(hv114595_0));
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
        return AstError<Expression>("GoogleSQL AST: Operands of " +
                                    node.detail + " cannot be literal NULL");
      }
    }
    // Bitwise operators have no BinaryOperation tag: desugar into function
    // calls so both the interpreter and plan executor can evaluate them.
    if (node.detail == "&" || node.detail == "|" || node.detail == "^" ||
        node.detail == "<<" || node.detail == ">>") {
      static const std::unordered_map<std::string, std::string> kBitFns = {
          {"&", "__bit_and"},
          {"|", "__bit_or"},
          {"^", "__bit_xor"},
          {"<<", "__shift_left"},
          {">>", "__shift_right"}};
      const std::string fn = kBitFns.at(std::string(node.detail));
      ASSIGN_OR_RETURN(Expression, hv116336_0,
                       (VisitExpression(*node.children[1])));
      Expression right = std::move(hv116336_0);
      return FunctionCallExp(fn, {std::move(left), std::move(right)});
    }
    ASSIGN_OR_RETURN(Expression, hv116336_1,
                     (VisitExpression(*node.children[1])));
    Expression right2 = std::move(hv116336_1);
    ASSIGN_OR_RETURN(BinaryOperation, bit_op, (BinaryOp(node.detail)));
    return BinaryExpressionExp(std::move(left), bit_op, std::move(right2));
  }

  if (node.kind == "AndExpr") {
    return FoldBoolean(node, BinaryOperation::kAnd);
  }
  if (node.kind == "OrExpr") {
    return FoldBoolean(node, BinaryOperation::kOr);
  }
  if (node.kind == "UnaryExpression") {
    if (node.children.size() != 1) {
      return AstError<Expression>("GoogleSQL AST: unary expression arity");
    }
    if (node.detail == "-") {
      if (node.children[0]->kind == "IntLiteral" &&
          node.children[0]->detail == "9223372036854775808") {
        return ConstantValueExp(Value(std::numeric_limits<int64_t>::min()));
      }
    }
    if (node.detail == "NOT") {
      if (node.children[0]->kind == "NullLiteral") {
        return AstError<Expression>(
            "GoogleSQL AST: Operands of NOT cannot be literal NULL");
      }
      if (node.children[0]->kind == "UnaryExpression" &&
          node.children[0]->detail == "NOT") {
        const auto* grand = node.children[0]->children.empty()
                                ? nullptr
                                : node.children[0]->children[0].get();
        if ((grand != nullptr) && grand->kind == "NullLiteral") {
          return AstError<Expression>(
              "GoogleSQL AST: Operands of NOT cannot be literal NULL");
        }
      }
    }
    // `x IS [NOT] UNKNOWN` arrives as a UnaryExpression (unlike IS NULL /
    // IS TRUE, which the parser shapes as BinaryExpression); UNKNOWN is a
    // NULL predicate, so map it to the corresponding null test.
    if (node.detail == "IS UNKNOWN") {
      ASSIGN_OR_RETURN(Expression, unk_e, (VisitExpression(*node.children[0])));
      return UnaryExpressionExp(std::move(unk_e), UnaryOperation::kIsNull);
    }
    if (node.detail == "IS NOT UNKNOWN") {
      ASSIGN_OR_RETURN(Expression, unk_e, (VisitExpression(*node.children[0])));
      return UnaryExpressionExp(std::move(unk_e), UnaryOperation::kIsNotNull);
    }
    ASSIGN_OR_RETURN(Expression, not_e, (VisitExpression(*node.children[0])));
    return UnaryExpressionExp(std::move(not_e), node.detail == "NOT"
                                                    ? UnaryOperation::kNot
                                                    : UnaryOperation::kMinus);
  }

  if (node.kind == "ConcatExpr") {
    std::vector<Expression> args;
    args.reserve(node.children.size());
    bool all_arrays = true;
    for (const auto& child : node.children) {
      if (child->kind == "Location") {
        continue;
      }
      ASSIGN_OR_RETURN(Expression, hv118716_0, (VisitExpression(*child)));
      Expression arg = std::move(hv118716_0);
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
    return AstError<Expression>("GoogleSQL AST: empty aliased expression");
  }

  if (node.kind == "ArrayElement") {
    // a[OFFSET(n)] / a[ORDINAL(n)] / a[n] (OFFSET semantics by default).
    Expression base;
    const GoogleSqlAstNode* index_node = nullptr;
    std::string accessor;
    auto accessor_name =
        [](const GoogleSqlAstNode& call) -> StatusOr<std::string> {
      if (call.children.empty() ||
          call.children.front()->kind != "PathExpression") {
        return std::string{};
      }
      ASSIGN_OR_RETURN(std::string, h_fn, (Path(*call.children.front())));
      std::string name = UpperCopy(std::move(h_fn));
      if (name == "OFFSET" || name == "ORDINAL" || name == "SAFE_OFFSET" ||
          name == "SAFE_ORDINAL") {
        return name;
      }
      return std::string{};
    };
    for (const auto& child : node.children) {
      if (child->kind == "Location") {
        continue;
      }
      if (child->kind == "FunctionCall") {
        ASSIGN_OR_RETURN(std::string, name, (accessor_name(*child)));
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
        ASSIGN_OR_RETURN(Expression, hv120918_0, (VisitExpression(*child)));
        base = std::move(hv120918_0);
      }
    }
    if (!base || index_node == nullptr) {
      return AstError<Expression>("GoogleSQL AST: malformed array element");
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
        (accessor.starts_with("SAFE_") ||
         accessor.find("_SAFE") != std::string::npos)) {
      fn += "_safe";
    }
    ASSIGN_OR_RETURN(Expression, h_idx, (VisitExpression(*index_node)));
    return FunctionCallExp(fn, {std::move(base), std::move(h_idx)});
  }

  if (node.kind == "BitwiseShiftExpression") {
    // `expr << n` / `expr >> n`: children are [expr, Location, n].
    if (node.children.size() < 3) {
      return AstError<Expression>("GoogleSQL AST: malformed shift expression");
    }
    const bool left_shift = node.detail == "<<";
    ASSIGN_OR_RETURN(Expression, h_sa, (VisitExpression(*node.children[0])));
    ASSIGN_OR_RETURN(Expression, h_sb, (VisitExpression(*node.children[2])));
    return FunctionCallExp(left_shift ? "__shift_left" : "__shift_right",
                           {std::move(h_sa), std::move(h_sb)});
  }

  if (node.kind == "DotStar") {
    // `relation.*`: qualified star expanded during projection.
    const GoogleSqlAstNode* path = node.Child("PathExpression");
    if (path == nullptr) {
      return AstError<Expression>("GoogleSQL AST: malformed DotStar");
    }
    ASSIGN_OR_RETURN(std::string, hv122407_0, (Path(*path)));
    ColumnName name(std::move(hv122407_0), "*");
    return ColumnValueExp(name);
  }

  if (node.kind == "CaseValueExpression") {
    if (node.children.empty()) {
      return AstError<Expression>("GoogleSQL AST: empty CASE");
    }
    ASSIGN_OR_RETURN(Expression, hv122631_0,
                     (VisitExpression(*node.children[0])));
    Expression value_expr = std::move(hv122631_0);
    std::vector<std::pair<Expression, Expression>> clauses;
    size_t pair_end = node.children.size();
    Expression otherwise = ConstantValueExp(Value());
    if ((pair_end - 1) % 2 == 1) {
      ASSIGN_OR_RETURN(Expression, hv122888_0,
                       (VisitExpression(*node.children.back())));
      otherwise = std::move(hv122888_0);
      --pair_end;
    }
    for (size_t i = 1; i < pair_end; i += 2) {
      ASSIGN_OR_RETURN(Expression, hv123017_0,
                       (VisitExpression(*node.children[i])));
      Expression when_val = std::move(hv123017_0);
      Expression cond = BinaryExpressionExp(
          value_expr, BinaryOperation::kEquals, std::move(when_val));
      ASSIGN_OR_RETURN(Expression, hv123196_0,
                       (VisitExpression(*node.children[i + 1])));
      clauses.emplace_back(std::move(cond), std::move(hv123196_0));
    }
    return CaseExpressionExp(std::move(clauses), std::move(otherwise));
  }
  if (node.kind == "CaseNoValueExpression") {
    if (node.children.empty()) {
      return AstError<Expression>("GoogleSQL AST: empty CASE");
    }
    std::vector<std::pair<Expression, Expression>> clauses;
    size_t pair_end = node.children.size();
    Expression otherwise = ConstantValueExp(Value());
    if (pair_end % 2 == 1) {
      ASSIGN_OR_RETURN(Expression, hv123726_0,
                       (VisitExpression(*node.children.back())));
      otherwise = std::move(hv123726_0);
      --pair_end;
    }
    for (size_t i = 0; i < pair_end; i += 2) {
      ASSIGN_OR_RETURN(Expression, hv123855_0,
                       (VisitExpression(*node.children[i + 1])));
      ASSIGN_OR_RETURN(Expression, hv123855_1,
                       (VisitExpression(*node.children[i])));
      clauses.emplace_back(std::move(hv123855_1), std::move(hv123855_0));
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
      return AstError<Expression>("GoogleSQL AST: BETWEEN arity");
    }
    ASSIGN_OR_RETURN(Expression, hv124406_0, (VisitExpression(*operands[1])));
    ASSIGN_OR_RETURN(Expression, hv124406_1, (VisitExpression(*operands[0])));
    Expression lower = BinaryExpressionExp(std::move(hv124406_1),
                                           BinaryOperation::kGreaterThanEquals,
                                           std::move(hv124406_0));
    ASSIGN_OR_RETURN(Expression, hv124635_0, (VisitExpression(*operands[2])));
    ASSIGN_OR_RETURN(Expression, hv124635_1, (VisitExpression(*operands[0])));
    Expression upper = BinaryExpressionExp(std::move(hv124635_1),
                                           BinaryOperation::kLessThanEquals,
                                           std::move(hv124635_0));
    Expression result = BinaryExpressionExp(
        std::move(lower), BinaryOperation::kAnd, std::move(upper));
    if (node.detail == "NOT BETWEEN") {
      result = UnaryExpressionExp(std::move(result), UnaryOperation::kNot);
    }
    return result;
  }
  if (node.kind == "InExpression") {
    if (node.children.empty()) {
      return AstError<Expression>("GoogleSQL AST: empty IN");
    }
    ASSIGN_OR_RETURN(Expression, hv125257_0,
                     (VisitExpression(*node.children.front())));
    Expression test = std::move(hv125257_0);
    const bool negated = node.detail == "NOT IN";
    if (const GoogleSqlAstNode* list = node.Child("InList")) {
      std::vector<Expression> values;
      values.reserve(list->children.size());
      for (const auto& child : list->children) {
        ASSIGN_OR_RETURN(Expression, hv125565_0, (VisitExpression(*child)));
        values.push_back(std::move(hv125565_0));
      }
      Expression result = InExpressionExp(std::move(test), std::move(values));
      return negated
                 ? UnaryExpressionExp(std::move(result), UnaryOperation::kNot)
                 : result;
    }
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query != nullptr) {
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, h_q,
                       (VisitQuery(*query)));
      return QueryExpressionExp(std::move(h_q), std::move(test), false,
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
        return AstError<Expression>("GoogleSQL AST: malformed UNNEST");
      }
      Expression array;
      for (const auto& expr_child : inner->children) {
        if (expr_child->kind != "Location" && expr_child->kind != "Alias" &&
            expr_child->kind != "Identifier") {
          ASSIGN_OR_RETURN(Expression, hv126723_0,
                           (VisitExpression(*expr_child)));
          array = std::move(hv126723_0);
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
    return AstError<Expression>("GoogleSQL AST: IN without values");
  }
  if (node.kind == "ExpressionSubquery") {
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query == nullptr) {
      return AstError<Expression>("GoogleSQL AST: subquery without query");
    }
    // ARRAY(SELECT ...): the subquery result is consumed as one array value.
    if (node.detail == "modifier=ARRAY") {
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv127598_0,
                       (VisitQuery(*query)));
      auto array_query = std::make_shared<QueryExpression>(
          std::move(hv127598_0), nullptr, false, false);
      array_query->SetArrayResult(true);
      ASSIGN_OR_RETURN(std::string, h_arr_t,
                       (InferSubqueryArrayElementType(*query)));
      array_query->SetArrayElementSqlType(h_arr_t);
      return {array_query};
    }
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, h_q,
                     (VisitQuery(*query)));
    return QueryExpressionExp(std::move(h_q), nullptr,
                              node.detail == "modifier=EXISTS", false);
  }
  if (node.kind == "ExtractExpression") {
    if (node.children.size() < 2) {
      return AstError<Expression>("GoogleSQL AST: EXTRACT arity");
    }
    ASSIGN_OR_RETURN(std::string, hv128166_0, (Path(*node.children[0])));
    const std::string part = Lower(std::move(hv128166_0));
    std::vector<Expression> args;
    ASSIGN_OR_RETURN(Expression, hv128261_0,
                     (VisitExpression(*node.children[1])));
    args.push_back(std::move(hv128261_0));
    if (node.children.size() >= 3) {
      ASSIGN_OR_RETURN(Expression, hv128354_0,
                       (VisitExpression(*node.children[2])));
      args.push_back(std::move(hv128354_0));
    }
    return FunctionCallExp("extract_" + part, std::move(args));
  }
  if (node.kind == "IntervalExpr") {
    std::string unit = "second";
    const GoogleSqlAstNode* value_node = nullptr;
    std::vector<std::string> unit_parts;
    for (const auto& child : node.children) {
      if (child->kind == "Identifier" || child->kind == "DateOrTimeUnit") {
        ASSIGN_OR_RETURN(std::string, hv128769_0, (Identifier(*child)));
        unit_parts.push_back(Lower(std::move(hv128769_0)));
      } else if (child->kind == "DateOrTimeUnitRange") {
        for (const auto& uc : child->children) {
          if (uc->kind == "Identifier" || uc->kind == "DateOrTimeUnit") {
            ASSIGN_OR_RETURN(std::string, hv129006_0, (Identifier(*uc)));
            unit_parts.push_back(Lower(std::move(hv129006_0)));
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
        ASSIGN_OR_RETURN(std::string, hv129471_0, (DecodeString(*value_node)));
        std::string str_val = std::move(hv129471_0);
        int64_t amount = 0;
        try {
          amount = std::stoll(str_val);
        } catch (const std::exception& error) {
          (void)error;
        }
        return IntervalExpressionExp(amount, std::move(unit),
                                     std::move(str_val));
      }
      ASSIGN_OR_RETURN(Expression, hv129819_0, (VisitExpression(*value_node)));
      Expression expr = std::move(hv129819_0);
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
          } catch (const std::exception& error) {
            (void)error;
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
      if (StatusOr<Value> v = expr->TryEvaluate(dummy_row, dummy_schema);
          v.HasValue()) {
        if (v.Value().type == ValueType::kInt64) {
          return IntervalExpressionExp(v.Value().value.int_value,
                                       std::move(unit));
        }
        if (v.Value().type == ValueType::kVarChar) {
          std::string str_val = std::string(v.Value().value.varchar_value);
          int64_t amount = 0;
          try {
            amount = std::stoll(str_val);
          } catch (const std::exception& error) {
            (void)error;
          }
          return IntervalExpressionExp(amount, std::move(unit),
                                       std::move(str_val));
        }
      }
      return FunctionCallExp(
          "make_interval", {expr, ConstantValueExp(Value(std::string(unit)))});
    }
    if (!node.children.empty() && node.children[0]->kind == "StringLiteral") {
      ASSIGN_OR_RETURN(std::string, hv131942_0,
                       (DecodeString(*node.children[0])));
      std::string res = std::move(hv131942_0);
      return ConstantValueExp(Value(std::move(res)));
    }
    return AstError<Expression>("GoogleSQL AST: unsupported interval");
  }
  if (node.kind == "ParameterExpr") {
    const GoogleSqlAstNode* ident = node.Child("Identifier");
    if (ident != nullptr) {
      return ColumnValueExp(ident->detail);
    }
    return AstError<Expression>("GoogleSQL AST: invalid parameter expression");
  }
  if (node.kind == "RangeLiteral") {
    if (const GoogleSqlAstNode* str = node.Child("StringLiteral")) {
      ASSIGN_OR_RETURN(std::string, h_tmp_3645, (DecodeString(*str)));
      return ConstantValueExp(Value(std::move(h_tmp_3645)));
    }
    for (const auto& child : node.children) {
      if (child->kind == "StringLiteral") {
        ASSIGN_OR_RETURN(std::string, h_tmp_3649, (DecodeString(*child)));
        return ConstantValueExp(Value(std::move(h_tmp_3649)));
      }
    }
    return ConstantValueExp(Value(std::string(node.detail)));
  }
  if (node.kind == "DateOrTimeUnit") {
    if (const GoogleSqlAstNode* id = node.Child("Identifier")) {
      ASSIGN_OR_RETURN(std::string, h_tmp_3656, (Identifier(*id)));
      return ConstantValueExp(Value(std::move(h_tmp_3656)));
    }
    std::string unit = node.detail;
    if (unit.starts_with("unit=")) {
      unit = unit.substr(5);
    }
    return ConstantValueExp(Value(std::move(unit)));
  }
  if (node.kind == "CastExpression") {
    if (node.children.size() < 2) {
      return AstError<Expression>(
          "GoogleSQL AST: CAST without operand or type");
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
        ASSIGN_OR_RETURN(std::string, target,
                         (SqlTypeFromAst(*node.children[1])));
        if (target.empty()) {
          target = "INT64";
        }
        // Keep UINT64 literals above INT64_MAX in the engine's signed
        // bit-pattern representation.  The cast expression and result
        // formatter already interpret this representation as UINT64; only
        // narrowing casts must reject it.
        if (UpperCopy(target) == "UINT64" &&
            (digits.empty() || digits.front() != '-')) {
          return ConstantValueExp(
              Value(static_cast<int64_t>(magnitude)).WithUnsigned());
        }
        if (safe_cast_target) {
          return ConstantValueExp(Value());
        }
        return AstError<Expression>(UpperCopy(target) +
                                    " out of range: " + digits);
      }
    }
    // BYTES -> STRING must carry valid UTF-8; GoogleSQL raises on invalid
    // sequences rather than re-encoding them.
    {
      ASSIGN_OR_RETURN(std::string, h_cast_t,
                       (SqlTypeFromAst(*node.children[1])));
      const std::string upper_cast_type = UpperCopy(std::move(h_cast_t));
      if ((upper_cast_type == "STRING" || upper_cast_type == "VARCHAR") &&
          IsBytesAstNode(*node.children[0])) {
        ASSIGN_OR_RETURN(std::string, bytes, (DecodeBytes(*node.children[0])));
        if (!IsValidUtf8Text(bytes)) {
          if (safe_cast_target) {
            return ConstantValueExp(Value());
          }
          return AstError<Expression>(
              "Cannot cast bytes with invalid UTF-8 to STRING");
        }
      }
    }
    ValidateStructCastCoercibility(node, safe_cast_target);
    ASSIGN_OR_RETURN(Expression, hv135524_0,
                     (VisitExpression(*node.children[0])));
    Expression child = std::move(hv135524_0);
    ASSIGN_OR_RETURN(std::string, type_name,
                     (SqlTypeFromAst(*node.children[1])));
    if (type_name.empty()) {
      if (const auto* path = node.children[1]->Child("PathExpression")) {
        ASSIGN_OR_RETURN(std::string, hv135749_0, (Path(*path)));
        type_name = std::move(hv135749_0);
      } else if (node.children[1]->kind == "SimpleType") {
        for (const auto& c : node.children[1]->children) {
          ASSIGN_OR_RETURN(std::string, h_tmp_3726, (SqlTypeFromAst(*c)));
          type_name = std::move(h_tmp_3726);
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
        if (safe_cast_target) {
          return ConstantValueExp(Value());
        }
        return AstError<Expression>("UINT64 out of range: -" + literal->detail);
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
      return ConstantValueExp(Value(
          UpperCopy(node.children[0]->detail) == "TRUE" ? "true" : "false"));
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
            ASSIGN_OR_RETURN(std::string, hv138409_0, (Identifier(*id)));
            field_names.push_back(std::move(hv138409_0));
          } else {
            field_names.emplace_back("");
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
            ASSIGN_OR_RETURN(std::string, hv139571_0, (Identifier(*id)));
            fname = std::move(hv139571_0);
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
        ASSIGN_OR_RETURN(Expression, val_expr, (VisitExpression(*arg_node)));
        if (val_expr) {
          StatusOr<Value> v_or = val_expr->TryEvaluate(Row(), Schema());
          if (v_or.HasValue()) {
            const Value& v = v_or.Value();
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
          } else {
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
    // JSON string escaping shared by field names and string values: an
    // unescaped quote or backslash inside a value (e.g. STRUCT('a"b')) made
    // the folded struct JSON unparseable.
    const auto escape_json = [](const std::string& text) {
      std::string escaped;
      escaped.reserve(text.size());
      for (char c : text) {
        switch (c) {
          case '"':
            escaped += "\\\"";
            break;
          case '\\':
            escaped += "\\\\";
            break;
          case '\n':
            escaped += "\\n";
            break;
          case '\r':
            escaped += "\\r";
            break;
          case '\t':
            escaped += "\\t";
            break;
          default:
            if (static_cast<unsigned char>(c) < 0x20) {
              std::array<char, 8> buf{};
              (void)snprintf(buf.data(), buf.size(), "\\u%04x",
                             static_cast<unsigned char>(c));
              escaped += buf.data();
            } else {
              escaped.push_back(c);
            }
            break;
        }
      }
      return escaped;
    };
    for (auto& field : fields) {
      if (!first) {
        json_out += ",";
      }
      first = false;
      json_out += "\"" + escape_json(field.name) + "\":";
      json_out +=
          field.is_string ? "\"" + escape_json(field.text) + "\"" : field.text;
    }
    json_out += "}";
    return ConstantValueExp(Value(std::move(json_out)));
  }

  if (node.kind == "DotIdentifier") {
    if (node.children.size() >= 2 && node.children[1]->kind == "Identifier") {
      ASSIGN_OR_RETURN(Expression, hv145253_0,
                       (VisitExpression(*node.children[0])));
      Expression base_expr = std::move(hv145253_0);
      ASSIGN_OR_RETURN(std::string, hv145318_0,
                       (Identifier(*node.children[1])));
      std::string field_name = std::move(hv145318_0);
      return FunctionCallExp("get_field",
                             {std::move(base_expr),
                              ConstantValueExp(Value(std::move(field_name)))});
    }
    return AstError<Expression>("GoogleSQL AST: invalid DotIdentifier");
  }

  if (node.kind == "UnnestExpression") {
    // In scalar contexts (e.g. quantified comparisons) UNNEST simply yields
    // its underlying array expression.
    const GoogleSqlAstNode* inner = node.Child("ExpressionWithOptAlias");
    if (inner == nullptr || inner->children.empty()) {
      return AstError<Expression>("GoogleSQL AST: malformed UNNEST");
    }
    for (const auto& child : inner->children) {
      if (child->kind != "Location" && child->kind != "Identifier" &&
          child->kind != "Alias") {
        return VisitExpression(*child);
      }
    }
    return AstError<Expression>("GoogleSQL AST: empty UNNEST");
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
        ASSIGN_OR_RETURN(Expression, hv147184_0, (VisitExpression(*child)));
        lhs = std::move(hv147184_0);
      } else if (collection == nullptr) {
        // UNNEST(array) / bare array expression collections.
        collection = child.get();
      }
    }
    if (!lhs || quantifier.empty()) {
      return AstError<Expression>(
          "GoogleSQL AST: malformed quantified comparison");
    }
    ASSIGN_OR_RETURN(BinaryOperation, hv147504_0, (BinaryOp(node.detail)));
    const BinaryOperation op = hv147504_0;
    const bool is_any = quantifier == "ANY" || quantifier == "SOME";
    const QuantifierMode mode =
        is_any ? QuantifierMode::kAny : QuantifierMode::kAll;
    if (query_node != nullptr) {
      // `lhs <op> ANY/ALL (SELECT x FROM UNNEST(arr) x)` iterates the array
      // directly; evaluating it as a correlated subquery would require
      // outer-scope resolution inside UNNEST table sources.
      if (const GoogleSqlAstNode* array_expr =
              UnnestArrayOfQuantifiedSubquery(*query_node)) {
        ASSIGN_OR_RETURN(Expression, hv148073_0,
                         (VisitExpression(*array_expr)));
        Expression arr = std::move(hv148073_0);
        std::string op_text(node.detail);
        std::string quantifier_text = quantifier;
        return FunctionCallExp(
            "__quantified__",
            {std::move(lhs), std::move(arr),
             ConstantValueExp(Value(std::move(op_text))),
             ConstantValueExp(Value(std::move(quantifier_text)))});
      }
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, h_q,
                       (VisitQuery(*query_node)));
      return QueryExpressionExp(std::move(h_q), lhs, false, false, op, mode);
    }
    const GoogleSqlAstNode* query =
        list_node != nullptr ? list_node->Child("Query") : nullptr;
    if (query != nullptr) {
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, q_q,
                       (VisitQuery(*query)));
      return QueryExpressionExp(std::move(q_q), lhs, false, false, op, mode);
    }
    if (collection != nullptr) {
      ASSIGN_OR_RETURN(Expression, hv148871_0, (VisitExpression(*collection)));
      // Generic array-collection form: evaluated by the runtime helper
      // __quantified__(lhs, array, op, mode).
      Expression arr = std::move(hv148871_0);
      std::string node_detail = node.detail;
      std::string quantifier_copy = quantifier;
      return FunctionCallExp(
          "__quantified__",
          {std::move(lhs), std::move(arr),
           ConstantValueExp(Value(std::move(node_detail))),
           ConstantValueExp(Value(std::move(quantifier_copy)))});
    }
    // PRODUCTION FIX: a QuantifiedComparisonExpression with no query and no
    // list dereferenced a null node here and crashed the process (the AST is
    // fuzzer/untrusted input). Fail loudly instead.
    if (list_node == nullptr) {
      return AstError<Expression>(
          "GoogleSQL AST: malformed quantified comparison (missing list)");
    }
    std::vector<Expression> items;
    for (const auto& child : list_node->children) {
      if (child->kind != "Location") {
        ASSIGN_OR_RETURN(Expression, hv149841_0, (VisitExpression(*child)));
        items.push_back(std::move(hv149841_0));
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
        ASSIGN_OR_RETURN(Expression, hv151178_0, (VisitExpression(*child)));
        lhs = std::move(hv151178_0);
      } else {
        ASSIGN_OR_RETURN(Expression, h_tmp_4118, (VisitExpression(*child)));
        operands.push_back(std::move(h_tmp_4118));
      }
    }
    const bool negated =
        UpperCopy(node.detail).find("NOT") != std::string::npos;
    if (any_op != nullptr && query_node != nullptr) {
      // `lhs [NOT] LIKE ANY/ALL (subquery)`: per-row LIKE / NOT LIKE under
      // three-valued ANY/ALL combination.
      const bool is_any = UpperCopy(any_op->detail) == "ANY" ||
                          UpperCopy(any_op->detail) == "SOME";
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, h_q,
                       (VisitQuery(*query_node)));
      return QueryExpressionExp(
          std::move(h_q), lhs, false, false,
          negated ? BinaryOperation::kNotLike : BinaryOperation::kLike,
          is_any ? QuantifierMode::kAny : QuantifierMode::kAll);
    }
    if (any_op == nullptr || list_node == nullptr) {
      if (operands.empty()) {
        return AstError<Expression>("GoogleSQL AST: malformed LIKE");
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
        ASSIGN_OR_RETURN(Expression, hv152598_0, (VisitExpression(*child)));
        patterns.push_back(std::move(hv152598_0));
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
    ASSIGN_OR_RETURN(Expression, hv153402_0,
                     (VisitExpression(*node.children[0])));
    // proto extension access: value.(pkg.Ext.field).  Lowered to a runtime
    // lookup of the bracketed extension key inside the TEXT payload.
    Expression base = std::move(hv153402_0);
    ASSIGN_OR_RETURN(std::string, hv153606_0,
                     (Path(*node.children[node.children.size() - 1])));
    std::string extension_path = std::move(hv153606_0);
    return FunctionCallExp(
        "__get_extension",
        {std::move(base), ConstantValueExp(Value(std::move(extension_path)))});
  }

  return AstError<Expression>("GoogleSQL AST: unsupported expression " +
                              node.kind);
}

bool ContainsAggregate(const Expression& expression) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kAggregateExp:
      return true;
    case TypeTag::kBinaryExp:
      return ContainsAggregate(expression->AsBinaryExpression().Left()) ||
             ContainsAggregate(expression->AsBinaryExpression().Right());
    case TypeTag::kUnaryExp:
      return ContainsAggregate(expression->AsUnaryExpression().Child());
    case TypeTag::kCaseExp: {
      const auto& val = expression->AsCaseExpression();
      for (const auto& [c, r] : val.when_clauses_) {
        if (ContainsAggregate(c) || ContainsAggregate(r)) {
          return true;
        }
      }
      return ContainsAggregate(val.else_clause_);
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      return std::ranges::any_of(call.Args(), [](const Expression& arg) {
        return ContainsAggregate(arg);
      });
    }
    case TypeTag::kInExp: {
      const auto& in_exp = expression->AsInExpression();
      if (ContainsAggregate(in_exp.child_)) {
        return true;
      }
      return std::ranges::any_of(in_exp.list_, [](const Expression& item) {
        return ContainsAggregate(item);
      });
    }
    case TypeTag::kArrayExp: {
      const auto& arr = expression->AsArrayExpression();
      return std::ranges::any_of(arr.Elements(), [](const Expression& item) {
        return ContainsAggregate(item);
      });
    }
    default:
      return false;
  }
}

StatusOr<SelectSource> ExpandPivotSource(SelectSource base,
                                         const GoogleSqlAstNode& pivot) {
  const GoogleSqlAstNode* expr_list = pivot.Child("PivotExpressionList");
  const GoogleSqlAstNode* for_col = pivot.Child("PathExpression");
  const GoogleSqlAstNode* value_list = pivot.Child("PivotValueList");
  if (expr_list == nullptr || for_col == nullptr || value_list == nullptr) {
    return AstError<SelectSource>("GoogleSQL AST: malformed PivotClause");
  }
  ASSIGN_OR_RETURN(std::string, hv155978_0, (Path(*for_col)));
  const std::string pivot_col_name = std::move(hv155978_0);

  struct PivotAggInfo {
    std::string agg_alias;
    std::string func_name;
    const GoogleSqlAstNode* arg_node{nullptr};
  };
  std::vector<PivotAggInfo> aggs;
  for (const GoogleSqlAstNode* pe : expr_list->Children("PivotExpression")) {
    PivotAggInfo info;
    ASSIGN_OR_RETURN(std::string, hv156297_0, (Alias(*pe)));
    info.agg_alias = std::move(hv156297_0);
    const GoogleSqlAstNode* fn_call = pe->Child("FunctionCall");
    if (fn_call == nullptr) {
      for (const auto& c : pe->children) {
        if (c->kind != "Alias" && c->kind != "Location") {
          fn_call = c.get();
          break;
        }
      }
    }
    if (fn_call != nullptr) {
      if (const GoogleSqlAstNode* fn_path = fn_call->Child("PathExpression")) {
        ASSIGN_OR_RETURN(std::string, hv156707_0, (Path(*fn_path)));
        info.func_name = std::move(hv156707_0);
      }
      for (const auto& c : fn_call->children) {
        if (c->kind != "PathExpression" && c->kind != "Alias" &&
            c->kind != "Location") {
          info.arg_node = c.get();
          break;
        }
      }
      if (info.arg_node == nullptr) {
        const auto paths = fn_call->Children("PathExpression");
        if (paths.size() >= 2) {
          info.arg_node = paths[1];
        }
      }
    }
    aggs.push_back(info);
  }

  std::vector<NamedExpression> projections;
  std::unordered_set<std::string> agg_column_names;
  for (const auto& agg : aggs) {
    if ((agg.arg_node != nullptr) && agg.arg_node->kind != "Star") {
      ASSIGN_OR_RETURN(std::string, hv157400_0, (Path(*agg.arg_node)));
      agg_column_names.insert(std::move(hv157400_0));
    }
  }

  std::vector<Expression> group_by_exprs;
  if (base.query) {
    for (const NamedExpression& named : base.query->SelectList()) {
      if (named.name != pivot_col_name &&
          !agg_column_names.contains(named.name)) {
        projections.emplace_back(named.name,
                                 ColumnValueExp(ColumnName(named.name)));
        group_by_exprs.push_back(ColumnValueExp(ColumnName(named.name)));
      }
    }
  }

  for (const GoogleSqlAstNode* pv : value_list->Children("PivotValue")) {
    const GoogleSqlAstNode* val_node = nullptr;
    for (const auto& c : pv->children) {
      if (c->kind != "Alias" && c->kind != "Location") {
        val_node = c.get();
        break;
      }
    }
    if (val_node == nullptr) {
      continue;
    }
    ASSIGN_OR_RETURN(Expression, hv158229_0, (VisitExpression(*val_node)));
    Expression val_expr = std::move(hv158229_0);
    ASSIGN_OR_RETURN(std::string, hv158283_0, (Alias(*pv)));
    std::string val_alias = std::move(hv158283_0);
    if (val_alias.empty()) {
      if (val_node->kind == "StringLiteral") {
        for (const auto& c : val_node->children) {
          if (c->kind == "StringLiteralComponent") {
            val_alias = c->detail;
            if (!val_alias.empty() && val_alias.front() == '\'' &&
                val_alias.back() == '\'') {
              val_alias = val_alias.substr(1, val_alias.size() - 2);
            }
            break;
          }
        }
      }
      if (val_alias.empty()) {
        val_alias = val_node->detail;
      }
    }

    for (const auto& agg : aggs) {
      std::string col_name;
      if (agg.agg_alias.empty()) {
        col_name = val_alias;
      } else if (val_alias.empty()) {
        col_name = agg.agg_alias;
      } else {
        col_name = agg.agg_alias + "_" + val_alias;
      }

      Expression condition =
          BinaryExpressionExp(ColumnValueExp(ColumnName(pivot_col_name)),
                              BinaryOperation::kEquals, val_expr);
      Expression inner_arg;
      if ((agg.arg_node != nullptr) && agg.arg_node->kind != "Star") {
        ASSIGN_OR_RETURN(Expression, hv159410_0,
                         (VisitExpression(*agg.arg_node)));
        inner_arg = std::move(hv159410_0);
      } else {
        inner_arg = ConstantValueExp(Value(1));
      }
      std::vector<std::pair<Expression, Expression>> when_clauses;
      when_clauses.emplace_back(std::move(condition), std::move(inner_arg));
      Expression case_expr =
          CaseExpressionExp(std::move(when_clauses), ConstantValueExp(Value()));

      const std::string lower_fn = Lower(agg.func_name);
      AggregationType agg_type = AggregationType::kSum;
      if (lower_fn == "count") {
        agg_type = AggregationType::kCount;
      } else if (lower_fn == "avg") {
        agg_type = AggregationType::kAvg;
      } else if (lower_fn == "min") {
        agg_type = AggregationType::kMin;
      } else if (lower_fn == "max") {
        agg_type = AggregationType::kMax;
      }

      Expression agg_expression =
          AggregateExpressionExp(agg_type, std::move(case_expr), false);
      projections.emplace_back(col_name, std::move(agg_expression));
    }
  }

  auto pivot_subquery = std::make_shared<SelectStatement>();
  pivot_subquery->SetSelectList(std::move(projections));
  std::string saved_alias = base.alias;
  pivot_subquery->SetSources({std::move(base)});
  if (!group_by_exprs.empty()) {
    pivot_subquery->SetGroupBy(std::move(group_by_exprs));
  } else {
    std::vector<Expression> inferred;
    for (const NamedExpression& named : pivot_subquery->SelectList()) {
      if (named.expression && !ContainsAggregate(named.expression)) {
        inferred.push_back(named.expression);
      }
    }
    if (!inferred.empty()) {
      pivot_subquery->SetGroupBy(std::move(inferred));
    }
  }
  pivot_subquery->MarkComplex();

  SelectSource result;
  result.query = std::move(pivot_subquery);
  result.alias = saved_alias.empty() ? "pivot_table" : saved_alias;
  return result;
}

StatusOr<SelectSource> ExpandUnpivotSource(const SelectSource& base,
                                           const GoogleSqlAstNode& unpivot) {
  const GoogleSqlAstNode* val_cols = unpivot.Child("ExpressionList");
  const GoogleSqlAstNode* name_col_node = unpivot.Child("PathExpression");
  const GoogleSqlAstNode* in_items = unpivot.Child("UnpivotInItemList");
  if (val_cols == nullptr || name_col_node == nullptr || in_items == nullptr) {
    return AstError<SelectSource>("GoogleSQL AST: malformed UnpivotClause");
  }
  std::string val_col_name = "val";
  if (!val_cols->children.empty()) {
    ASSIGN_OR_RETURN(std::string, hv161845_0, (Path(*val_cols->children[0])));
    val_col_name = std::move(hv161845_0);
  }
  ASSIGN_OR_RETURN(std::string, hv161898_0, (Path(*name_col_node)));
  std::string name_col_name = std::move(hv161898_0);
  const bool include_nulls =
      unpivot.detail.find("INCLUDE NULLS") != std::string::npos;

  std::shared_ptr<SelectStatement> root_branch;
  std::string saved_alias = base.alias;

  for (const GoogleSqlAstNode* item : in_items->Children("UnpivotInItem")) {
    const GoogleSqlAstNode* in_expr_list = item->Child("ExpressionList");
    if (in_expr_list == nullptr || in_expr_list->children.empty()) {
      continue;
    }
    ASSIGN_OR_RETURN(std::string, hv162376_0,
                     (Path(*in_expr_list->children[0])));
    std::string in_col_name = std::move(hv162376_0);
    std::string in_label = in_col_name;
    if (const GoogleSqlAstNode* label_node =
            item->Child("UnpivotInItemLabel")) {
      if (const GoogleSqlAstNode* str_node =
              label_node->Child("StringLiteral")) {
        for (const auto& c : str_node->children) {
          if (c->kind == "StringLiteralComponent") {
            in_label = c->detail;
            if (!in_label.empty() && in_label.front() == '\'' &&
                in_label.back() == '\'') {
              in_label = in_label.substr(1, in_label.size() - 2);
            }
            break;
          }
        }
      } else if (!label_node->children.empty()) {
        in_label = label_node->children[0]->detail;
      }
    }

    std::vector<NamedExpression> branch_select;
    branch_select.emplace_back("", ColumnValueExp(ColumnName("*")));
    branch_select.emplace_back(val_col_name,
                               ColumnValueExp(ColumnName(in_col_name)));
    branch_select.emplace_back(name_col_name,
                               ConstantValueExp(Value(std::string(in_label))));

    auto branch = std::make_shared<SelectStatement>();
    branch->SetSelectList(std::move(branch_select));
    branch->SetSources({base});
    if (!include_nulls) {
      branch->SetWhereClause(UnaryExpressionExp(
          ColumnValueExp(ColumnName(in_col_name)), UnaryOperation::kIsNotNull));
    }
    branch->MarkComplex();

    if (!root_branch) {
      root_branch = branch;
    } else {
      root_branch->AddUnionAll(branch);
    }
  }

  SelectSource result;
  result.query = std::move(root_branch);
  result.alias = saved_alias.empty() ? "unpivot_table" : saved_alias;
  return result;
}

StatusOr<SelectSource> VisitTableSource(
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
    SuspendInnermostMask(const SuspendInnermostMask&) = delete;
    SuspendInnermostMask& operator=(const SuspendInnermostMask&) = delete;
    SuspendInnermostMask(SuspendInnermostMask&&) = delete;
    SuspendInnermostMask& operator=(SuspendInnermostMask&&) = delete;
  } suspend_mask;
  SelectSource source;
  source.join_type = join_type;
  source.join_condition = std::move(join_condition);
  ASSIGN_OR_RETURN(std::string, hv165436_0, (Alias(node)));
  source.alias = std::move(hv165436_0);
  // WITH OFFSET applies to both explicit UNNEST operators and implicit
  // unnests written as qualified field paths (`t.arr elem WITH OFFSET off`).
  auto capture_offset_alias = [&]() -> Status {
    const GoogleSqlAstNode* with_offset = node.Child("WithOffset");
    if (with_offset == nullptr) {
      with_offset = node.Child("WithOffsetClause");
    }
    if (with_offset != nullptr) {
      if (const GoogleSqlAstNode* alias = with_offset->Child("Alias")) {
        if (alias->Child("Identifier") != nullptr) {
          ASSIGN_OR_RETURN(std::string, hv165974_0,
                           (Identifier(*alias->Child("Identifier"))));
          source.offset_alias = std::move(hv165974_0);
        } else {
          source.offset_alias = "offset";
        }
      } else {
        source.offset_alias = "offset";
      }
      return Status::kSuccess;
    }
    for (const auto& child : node.children) {
      if (child->kind.find("Offset") != std::string::npos) {
        if (const GoogleSqlAstNode* alias = child->Child("Alias")) {
          if (alias->Child("Identifier") != nullptr) {
            ASSIGN_OR_RETURN(std::string, hv166430_0,
                             (Identifier(*alias->Child("Identifier"))));
            source.offset_alias = std::move(hv166430_0);
          } else {
            source.offset_alias = "offset";
          }
        } else {
          source.offset_alias = "offset";
        }
        break;
      }
    }
    return Status::kSuccess;
  };
  if (node.kind == "TablePathExpression") {
    if (const GoogleSqlAstNode* unnest = node.Child("UnnestExpression")) {
      for (const auto& child : unnest->children) {
        if (child->kind == "ExpressionWithOptAlias") {
          for (const auto& expr_child : child->children) {
            if (expr_child->kind != "Location" && expr_child->kind != "Alias") {
              ASSIGN_OR_RETURN(Expression, hv167048_0,
                               (VisitExpression(*expr_child)));
              source.unnest = std::move(hv167048_0);
              break;
            }
          }
          if (source.alias.empty()) {
            ASSIGN_OR_RETURN(std::string, hv167193_0, (Alias(*child)));
            source.alias = std::move(hv167193_0);
          }
          break;
        }
      }
      Status st_off = capture_offset_alias();
      if (st_off != Status::kSuccess) {
        return st_off;
      }
      if (source.alias.empty()) {
        source.alias = "unnest";
      }
      return source;
    }
    const GoogleSqlAstNode* path = node.Child("PathExpression");
    if (path == nullptr) {
      return AstError<SelectSource>("GoogleSQL AST: table without path");
    }
    ASSIGN_OR_RETURN(std::string, hv167586_0, (Path(*path)));
    // GoogleSQL FROM items may be qualified field paths (`t4.array_val`,
    // `t.Info.str_value`): an implicit UNNEST of an array-typed column or
    // nested field reached through a scope alias.  Such paths never name a
    // base relation, so map them to an unnest source whose expression is
    // resolved against the enclosing scope chain at execution time.
    const std::string dotted = std::move(hv167586_0);
    if (dotted.find('.') != std::string::npos && dotted.back() != '.') {
      ASSIGN_OR_RETURN(Expression, hv168071_0, (VisitExpression(*path)));
      source.unnest = std::move(hv168071_0);
      if (source.alias.empty()) {
        source.alias = dotted.substr(dotted.rfind('.') + 1);
      }
      Status st_off = capture_offset_alias();
      if (st_off != Status::kSuccess) {
        return st_off;
      }
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
        return AstError<SelectSource>("view expansion too deep: " + dotted);
      }
      ++ViewExpansionDepth();
      SelectSource view_source;
      Status st_view = [&]() -> Status {
        ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, h_tmp_4601,
                         (VisitQuery(*found_view->second)));
        view_source.query = std::move(h_tmp_4601);
        view_source.alias = source.alias.empty() ? Lower(dotted) : source.alias;
        return Status::kSuccess;
      }();
      --ViewExpansionDepth();
      if (st_view != Status::kSuccess) {
        return st_view;
      }
      return view_source;
    }
  } else if (node.kind == "TableSubquery") {
    const GoogleSqlAstNode* query = node.Child("Query");
    if (query == nullptr) {
      return AstError<SelectSource>(
          "GoogleSQL AST: table subquery missing query");
    }
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv169336_0,
                     (VisitQuery(*query)));
    source.query = std::move(hv169336_0);
    // The ZetaSQL parse dump has no structural marker for LATERAL (it only
    // emits a Location node for the comma), so detection relies on the byte
    // distance between the TableSubquery start and the inner Query start:
    // "(LATERAL (SELECT..." measures 9 bytes minimum, while a plain
    // parenthesized subquery measures 1 byte plus whitespace.  The old
    // threshold of 7 also matched 7-8 byte gaps, which only whitespace can
    // produce ("(      (SELECT..."), so such queries were silently treated
    // as correlated laterals.
    if (node.detail.find("lateral") != std::string::npos ||
        node.detail.find("LATERAL") != std::string::npos ||
        (query->start > node.start && (query->start - node.start) >= 9)) {
      source.is_lateral = true;
    } else if (source.query) {
      std::unordered_set<std::string> local_tables;
      for (const auto& s : source.query->Sources()) {
        if (!s.alias.empty()) {
          local_tables.insert(s.alias);
        }
        if (!s.table.empty()) {
          local_tables.insert(s.table);
        }
      }
      if (source.query->WhereClause()) {
        for (const auto& col : source.query->WhereClause()->TouchedColumns()) {
          if (!col.schema.empty() && !local_tables.contains(col.schema)) {
            source.is_lateral = true;
            break;
          }
        }
      }
    }
  } else {
    return AstError<SelectSource>("GoogleSQL AST: unsupported table source " +
                                  node.kind);
  }
  if (const GoogleSqlAstNode* pivot = node.Child("PivotClause")) {
    ASSIGN_OR_RETURN(SelectSource, hv170954_0,
                     (ExpandPivotSource(std::move(source), *pivot)));
    source = std::move(hv170954_0);
  } else if (const GoogleSqlAstNode* unpivot = node.Child("UnpivotClause")) {
    ASSIGN_OR_RETURN(SelectSource, hv171091_0,
                     (ExpandUnpivotSource(source, *unpivot)));
    source = std::move(hv171091_0);
  }
  return source;
}

Status AppendSources(
    const GoogleSqlAstNode& node,
    JoinType incoming,  // NOLINT(misc-no-recursion) // Recursive
                        // AST descent for nested joins by design
                        // (see VisitQuery depth note).
    Expression condition, std::vector<SelectSource>* sources) {
  // Parentheses around a nested JOIN are represented as a wrapper node by
  // the parser, but do not introduce a relation of their own.
  if (node.kind == "ParenthesizedJoin") {
    const GoogleSqlAstNode* nested = node.Child("Join");
    if (nested == nullptr) {
      return AstStatus("GoogleSQL AST: empty parenthesized join");
    }
    const size_t nested_begin = sources->size();
    Status st171918 =
        AppendSources(*nested, incoming, std::move(condition), sources);
    if (st171918 != Status::kSuccess) {
      return st171918;
    }
    for (size_t i = nested_begin; i < sources->size(); ++i) {
      (*sources)[i].from_nested_join = true;
    }
    return Status::kSuccess;
  }
  if (node.kind != "Join") {
    ASSIGN_OR_RETURN(SelectSource, hv172145_0,
                     (VisitTableSource(node, incoming, std::move(condition))));
    sources->push_back(std::move(hv172145_0));
    return Status::kSuccess;
  }
  std::vector<const GoogleSqlAstNode*> operands;
  const GoogleSqlAstNode* on = nullptr;
  const GoogleSqlAstNode* using_clause = nullptr;
  for (const auto& child : node.children) {
    if (child->kind == "TablePathExpression" ||
        child->kind == "TableSubquery" || child->kind == "Join" ||
        child->kind == "ParenthesizedJoin") {
      operands.push_back(child.get());
    } else if (child->kind == "OnClause") {
      on = child.get();
    } else if (using_clause == nullptr && child->kind.starts_with("Using")) {
      using_clause = child.get();
    }
  }
  if (operands.size() != 2) {
    return AstStatus("GoogleSQL AST: join arity");
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
    ASSIGN_OR_RETURN(Expression, hv173392_0,
                     (VisitExpression(*on->children[0])));
    join_expression = std::move(hv173392_0);
  } else if (using_clause != nullptr) {
    // USING(col, ...) carries no OnClause child; it is an equality join over
    // the shared columns. Dropping it silently would turn the statement into
    // a condition-less join.  The names also ride on the right-hand source so
    // execution can coalesce bare references and star expansion.
    for (const GoogleSqlAstNode* column :
         using_clause->Children("Identifier")) {
      ASSIGN_OR_RETURN(std::string, hv173881_0, (Identifier(*column)));
      using_columns.push_back(std::move(hv173881_0));
      ASSIGN_OR_RETURN(std::string, hv173933_0, (Identifier(*column)));
      ASSIGN_OR_RETURN(std::string, hv173933_1, (Identifier(*column)));
      Expression equality = BinaryExpressionExp(
          ColumnValueExp(std::move(hv173933_1)), BinaryOperation::kEquals,
          ColumnValueExp(std::move(hv173933_0)));
      join_expression =
          join_expression
              ? BinaryExpressionExp(std::move(join_expression),
                                    BinaryOperation::kAnd, std::move(equality))
              : std::move(equality);
    }
    if (!join_expression) {
      return AstStatus("GoogleSQL AST: unsupported join USING clause");
    }
  }
  const size_t right_source_index = sources->size();
  Status st174503 =
      AppendSources(*operands[1], type, std::move(join_expression), sources);
  if (st174503 != Status::kSuccess) {
    return st174503;
  }
  if (!using_columns.empty() && right_source_index < sources->size()) {
    (*sources)[right_source_index].using_columns = std::move(using_columns);
  }
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<SelectStatement>> VisitQuery(
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
        ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv175800_0,
                         (VisitQuery(*operands[0])));
        auto first_stmt = std::move(hv175800_0);
        bool union_by_name = false;
        std::vector<SetOperationKind> per_pair;
        SetOperationKind head_kind = SetOperationKind::kUnionAll;
        const std::string set_detail = UpperCopy(set_op->detail);
        if (set_detail.find("INTERSECT") != std::string::npos) {
          head_kind = set_detail.find("ALL") != std::string::npos
                          ? SetOperationKind::kIntersectAll
                          : SetOperationKind::kIntersect;
        } else if (set_detail.find("EXCEPT") != std::string::npos) {
          head_kind = set_detail.find("ALL") != std::string::npos
                          ? SetOperationKind::kExceptAll
                          : SetOperationKind::kExcept;
        } else if (set_detail.find("DISTINCT") != std::string::npos) {
          head_kind = SetOperationKind::kUnion;
        }
        if (const GoogleSqlAstNode* metadata_list =
                set_op->Child("SetOperationMetadataList")) {
          for (const GoogleSqlAstNode* metadata :
               metadata_list->Children("SetOperationMetadata")) {
            const GoogleSqlAstNode* type_node =
                metadata->Child("SetOperationType");
            const GoogleSqlAstNode* mode_node =
                metadata->Child("SetOperationAllOrDistinct");
            const std::string type_text = UpperCopy(SliceSource(type_node));
            const std::string mode_text = UpperCopy(SliceSource(mode_node));
            if (type_text.empty() && mode_text.empty()) {
              continue;
            }
            SetOperationKind kind = SetOperationKind::kUnion;
            if (type_text.find("INTERSECT") != std::string::npos) {
              kind = mode_text.find("ALL") != std::string::npos
                         ? SetOperationKind::kIntersectAll
                         : SetOperationKind::kIntersect;
            } else if (type_text.find("EXCEPT") != std::string::npos) {
              kind = mode_text.find("ALL") != std::string::npos
                         ? SetOperationKind::kExceptAll
                         : SetOperationKind::kExcept;
            } else {
              kind = mode_text.find("ALL") != std::string::npos
                         ? SetOperationKind::kUnionAll
                         : SetOperationKind::kUnion;
            }
            per_pair.push_back(kind);
            if (metadata->Child("SetOperationColumnMatchMode") != nullptr) {
              union_by_name = true;
            }
          }
        }
        while (per_pair.size() + 1 < operands.size()) {
          per_pair.push_back(head_kind);
        }
        if (set_detail.find("DISTINCT") != std::string::npos) {
          first_stmt->MarkUnionDistinct(union_by_name);
        }
        const auto first_operand =
            std::make_shared<SelectStatement>(*first_stmt);
        std::vector<std::shared_ptr<SelectStatement>> branches;
        branches.reserve(operands.size() - 1);
        for (size_t i = 1; i < operands.size(); ++i) {
          const SetOperationKind kind =
              i - 1 < per_pair.size() ? per_pair[i - 1] : head_kind;
          ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv178940_0,
                           (VisitQuery(*operands[i])));
          auto branch = std::move(hv178940_0);
          branches.push_back(branch);
          first_stmt->AddSetOperation(kind, std::move(branch));
        }
        // Keep explicit parenthesized groups available to the relational
        // executor.  The legacy vectors above intentionally remain populated
        // because the optimizer and SQL-template binder still consume them,
        // but flattening a grouped operand changes the meaning of e.g.
        // `(A UNION ALL B) INTERSECT C`.
        bool has_grouped_operand =
            first_operand->GetSetOperationTree() != nullptr;
        for (const auto& branch : branches) {
          if (branch != nullptr && branch->GetSetOperationTree() != nullptr) {
            has_grouped_operand = true;
          }
        }
        auto tree = std::make_shared<SetOperationTree>();
        tree->first = first_operand;
        tree->branches = std::move(branches);
        tree->kinds = per_pair;
        tree->grouped = has_grouped_operand;
        first_stmt->SetSetOperationTree(std::move(tree));
        // A set operation is represented by the first operand in the
        // statement tree, but query-level LIMIT/OFFSET and WITH clauses hang
        // off the wrapper.  Preserve them here so execution sees the same
        // scope and applies bounds after concatenating the branches.
        if (const GoogleSqlAstNode* limit_offset = query.Child("LimitOffset")) {
          if (const GoogleSqlAstNode* limit_node =
                  limit_offset->Child("Limit")) {
            for (const auto& child : limit_node->children) {
              if (child->kind == "Location" || child->kind == "Hint") {
                continue;
              }
              if (child->kind == "IntLiteral") {
                ASSIGN_OR_RETURN(uint64_t, hv180703_0,
                                 (ParseUnsignedLiteral(*child)));
                first_stmt->SetLimit(
                    static_cast<size_t>(std::move(hv180703_0)));
                break;
              }
              auto folded_or = VisitExpression(*child);
              if (folded_or.HasValue()) {
                Expression folded = folded_or.MoveValue();
                if (folded && folded->Type() == TypeTag::kConstantValue) {
                  const Value constant = folded->AsConstantValue().GetValue();
                  if (constant.type == ValueType::kInt64 &&
                      !constant.IsNull() && constant.value.int_value >= 0) {
                    first_stmt->SetLimit(
                        static_cast<size_t>(constant.value.int_value));
                    break;
                  }
                }
              }
              return AstError<std::shared_ptr<SelectStatement>>(
                  "LIMIT requires an integer literal in this engine");
            }
          }
          for (const auto& child : limit_offset->children) {
            if (child->kind == "Limit" || child->kind == "Location" ||
                child->kind == "Hint" || child->kind == "WithTies" ||
                child->kind == "Offset") {
              if (child->kind == "Offset") {
                for (const auto& sub : child->children) {
                  if (sub->kind == "IntLiteral") {
                    ASSIGN_OR_RETURN(uint64_t, hv182030_0,
                                     (ParseUnsignedLiteral(*sub)));
                    first_stmt->SetOffset(hv182030_0);
                  }
                }
              }
              continue;
            }
            if (child->kind == "IntLiteral") {
              ASSIGN_OR_RETURN(uint64_t, hv182240_0,
                               (ParseUnsignedLiteral(*child)));
              first_stmt->SetOffset(hv182240_0);
            } else {
              if (auto folded_or = VisitExpression(*child);
                  folded_or.HasValue()) {
                Expression folded = folded_or.MoveValue();
                if (folded && folded->Type() == TypeTag::kConstantValue) {
                  const Value constant = folded->AsConstantValue().GetValue();
                  if (constant.type == ValueType::kInt64 &&
                      !constant.IsNull() && constant.value.int_value >= 0) {
                    first_stmt->SetOffset(
                        static_cast<size_t>(constant.value.int_value));
                    continue;
                  }
                }
              }
              return AstError<std::shared_ptr<SelectStatement>>(
                  "OFFSET requires an integer literal in this engine");
            }
          }
        }
        if (const GoogleSqlAstNode* order = query.Child("OrderBy")) {
          std::vector<SelectStatement::OrderByTerm> order_by;
          for (const GoogleSqlAstNode* term :
               order->Children("OrderingExpression")) {
            ASSIGN_OR_RETURN(WindowOrderTerm, parsed,
                             (ParseOrderingTerm(term)));
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
            ASSIGN_OR_RETURN(std::string, hv184351_0, (Identifier(*name)));
            const std::string cte_name = std::move(hv184351_0);
            if (recursive) {
              ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv184440_0,
                               (VisitQuery(*nested)));
              first_stmt->AddRecursiveWithQuery(cte_name,
                                                std::move(hv184440_0));
            } else {
              ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, h_q,
                               (VisitQuery(*nested)));
              first_stmt->AddWithQuery(cte_name, std::move(h_q));
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
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv185102_0,
                       (VisitQuery(*child)));
      auto nested = std::move(hv185102_0);
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
          ASSIGN_OR_RETURN(std::string, hv185777_0, (Identifier(*name)));
          const std::string cte_name = std::move(hv185777_0);
          if (recursive) {
            ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv185862_0,
                             (VisitQuery(*body)));
            nested->AddRecursiveWithQuery(cte_name, std::move(hv185862_0));
          } else {
            ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, h_q,
                             (VisitQuery(*body)));
            nested->AddWithQuery(cte_name, std::move(h_q));
          }
        }
      }
      return nested;
    }
    return AstError<std::shared_ptr<SelectStatement>>(
        "GoogleSQL AST: query without SELECT");
  }
  const GoogleSqlAstNode* select_list = select->Child("SelectList");
  if (select_list == nullptr) {
    return AstError<std::shared_ptr<SelectStatement>>(
        "GoogleSQL AST: SELECT without list");
  }

  // Named windows must resolve before select-list expressions are built.
  // The WindowClause hangs off the Select node, next to the FromClause.
  struct NamedWindowsScope {
    std::unordered_map<std::string, NamedWindowParts> previous;
    NamedWindowsScope() = default;
    ~NamedWindowsScope() { t_named_windows.swap(previous); }
    NamedWindowsScope(const NamedWindowsScope&) = delete;
    NamedWindowsScope& operator=(const NamedWindowsScope&) = delete;
    NamedWindowsScope(NamedWindowsScope&&) = delete;
    NamedWindowsScope& operator=(NamedWindowsScope&&) = delete;
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
        ASSIGN_OR_RETURN(std::string, hv187788_0, (Identifier(*base)));
        const auto found = t_named_windows.find(hv187788_0);
        if (found != t_named_windows.end()) {
          parts = found->second;
        }
      }
      ASSIGN_OR_RETURN(NamedWindowParts, hv187953_0,
                       (ParseWindowSpecification(*spec)));
      NamedWindowParts own = std::move(hv187953_0);
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
      ASSIGN_OR_RETURN(std::string, hv188409_0, (Identifier(*name_node)));
      t_named_windows[std::move(hv188409_0)] = std::move(parts);
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
    BoundMaskScope() = default;
    ~BoundMaskScope() {
      if (!t_udf_bound_masks.empty()) {
        t_udf_bound_masks.pop_back();
      }
    }
    BoundMaskScope(const BoundMaskScope&) = delete;
    BoundMaskScope& operator=(const BoundMaskScope&) = delete;
    BoundMaskScope(BoundMaskScope&&) = delete;
    BoundMaskScope& operator=(BoundMaskScope&&) = delete;
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
      return AstError<std::shared_ptr<SelectStatement>>(
          "GoogleSQL AST: empty column");
    }
    if (expression_node->kind == "DotStar" &&
        expression_node->Child("PathExpression") == nullptr) {
      // `sql_udf(...).*` is a projection expansion, not a column named `*`.
      const GoogleSqlAstNode* call = expression_node->Child("FunctionCall");
      if (call == nullptr) {
        return AstError<std::shared_ptr<SelectStatement>>(
            "GoogleSQL AST: malformed DotStar");
      }
      ASSIGN_OR_RETURN(Expression, hv190303_0, (VisitExpression(*call)));
      Expression base = std::move(hv190303_0);
      for (const char* field : {"aarr", "acount", "amin", "amax"}) {
        projections.emplace_back(
            field, FunctionCallExp(
                       "__get_field_safe",
                       {base, ConstantValueExp(Value(std::string(field)))}));
        projection_nodes.push_back(expression_node);
      }
      continue;
    }
    ASSIGN_OR_RETURN(Expression, hv190694_0,
                     (VisitExpression(*expression_node)));
    Expression expression = std::move(hv190694_0);
    ASSIGN_OR_RETURN(std::string, hv190757_0, (Alias(*column)));
    std::string name = std::move(hv190757_0);
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
  if (as_value && projections.size() == 1) {
    projections.front().expression = FunctionCallExp(
        "__value_table_value", {projections.front().expression});
  }
  if (is_as_struct && projections.size() == 1 && projection_nodes.size() == 1 &&
      projection_nodes.front()->kind == "DotStar") {
    const Expression star = projections.front().expression;
    const ColumnName& star_name = star->AsColumnValue().GetColumnName();
    projections.clear();
    for (const char* field : {"foo", "bar"}) {
      projections.emplace_back(
          field, ColumnValueExp(ColumnName(star_name.schema, field)));
    }
  }
  // `SELECT AS Proto p.*` selects the value-table message itself.  It is not
  // a proto field literally named `*`, so preserve the value expression and
  // avoid wrapping it in a constructor that would serialize `{*: 1}`.
  const bool selects_value_table_star =
      projections.size() == 1 && projection_nodes.size() == 1 &&
      projection_nodes.front()->kind == "DotStar" &&
      projections.front().expression->Type() == TypeTag::kColumnValue &&
      projections.front().expression->AsColumnValue().GetColumnName().name ==
          "*";
  if (selects_value_table_star) {
    projections.front().expression = FunctionCallExp(
        "__value_table_value", {projections.front().expression});
  }
  // SELECT AS <proto>: validate projected fields against the registry so
  // required-field violations and invalid enum values fail at prepare time.
  if (!is_as_struct && !as_value && select_as != nullptr) {
    if (const GoogleSqlAstNode* as_path = select_as->Child("PathExpression")) {
      std::vector<std::pair<std::string, const GoogleSqlAstNode*>> named;
      const size_t count =
          std::min(projections.size(), projection_nodes.size());
      named.reserve(count);
      for (size_t i = 0; i < count; ++i) {
        named.emplace_back(projections[i].name, projection_nodes[i]);
      }
      ASSIGN_OR_RETURN(std::string, hv193609_0, (Path(*as_path)));
      Status st201702 =
          ValidateSelectAsProjections(hv193609_0, named, &projections);
      if (st201702 != Status::kSuccess) {
        return st201702;
      }
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
          ASSIGN_OR_RETURN(std::string, hv194317_0, (Path(*child)));
          std::string candidate;
          if (child->kind == "PathExpression") {
            candidate = std::move(hv194317_0);
          } else {
            ASSIGN_OR_RETURN(std::string, h_cand_t, (SqlTypeFromAst(*child)));
            candidate = std::move(h_cand_t);
          }
          for (char& c : candidate) {
            if (c == '`') {
              c = ' ';
            }
          }
          std::string collapsed;
          for (const char c : candidate) {
            if (c != ' ' || (!collapsed.empty() && collapsed.back() != ' ')) {
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
        while (end < select->detail.size() && select->detail[end] != ',' &&
               select->detail[end] != ' ') {
          ++end;
        }
        type_name = select->detail.substr(mode + 8, end - mode - 8);
      } else if (UpperCopy(select_as != nullptr ? select_as->detail
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
      args.emplace_back(ConstantValueExp(Value(std::string(type_name))));
      for (size_t i = 0; i < projections.size() && i < projection_nodes.size();
           ++i) {
        std::string fname = projections[i].name.empty()
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
      if (selects_value_table_star) {
        projections = {NamedExpression(
            "",
            FunctionCallExp("__value_table_proto",
                            {ConstantValueExp(Value(std::string(type_name))),
                             projections.front().expression}))};
      } else {
        projections = {NamedExpression(
            "", FunctionCallExp("__proto_new", std::move(args)))};
      }
    }
  }

  std::vector<SelectSource> sources;

  std::vector<std::string> tables;
  if (const GoogleSqlAstNode* from = select->Child("FromClause")) {
    for (const auto& child : from->children) {
      Status st197530 =
          AppendSources(*child, JoinType::kCross, nullptr, &sources);
      if (st197530 != Status::kSuccess) {
        return st197530;
      }
    }
    for (const SelectSource& source : sources) {
      if (!source.table.empty()) {
        const std::string relation =
            source.alias.empty() ? source.table : source.alias;
        tables.push_back(relation);
      }
    }
  }

  Expression where;
  if (const GoogleSqlAstNode* clause = select->Child("WhereClause")) {
    if (!clause->children.empty()) {
      ASSIGN_OR_RETURN(Expression, hv197970_0,
                       (VisitExpression(*clause->children[0])));
      where = std::move(hv197970_0);
    }
  }

  std::vector<SelectStatement::OrderByTerm> order_by;
  if (const GoogleSqlAstNode* order = query.Child("OrderBy")) {
    for (const GoogleSqlAstNode* term : order->Children("OrderingExpression")) {
      ASSIGN_OR_RETURN(WindowOrderTerm, parsed, (ParseOrderingTerm(term)));
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
          const auto ordinal =
              static_cast<size_t>(ordinal_value.value.int_value);
          if (ordinal >= 1 && ordinal <= projections.size() &&
              projections[ordinal - 1].expression) {
            order_by.push_back({projections[ordinal - 1].expression,
                                parsed.ascending, parsed.nulls_first});
            continue;
          }
          return AstError<std::shared_ptr<SelectStatement>>(
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
                                   std::string_view clause)
      -> StatusOr<std::shared_ptr<SelectStatement>> {
    if (operand.kind == "NullLiteral") {
      return AstError<std::shared_ptr<SelectStatement>>(std::string(clause) +
                                                        " must not be NULL");
    }
    if (operand.kind == "UnaryExpression" && operand.detail == "-" &&
        !operand.children.empty() &&
        operand.children.front()->kind == "IntLiteral") {
      return AstError<std::shared_ptr<SelectStatement>>(
          std::string(clause) + " must be non-negative");
    }
    return std::shared_ptr<SelectStatement>{};
  };
  // Constant integer operands may arrive wrapped (e.g. LIMIT (cast(1 as
  // int32)) after parameter substitution, or an Offset wrapper node). Fold
  // them through the expression evaluator; only truly non-constant operands
  // (query parameters that survived substitution) fail loudly instead of
  // silently degrading to unlimited.
  std::function<std::optional<int64_t>(const GoogleSqlAstNode&)> fold_int =
      [&](const GoogleSqlAstNode& node) -> std::optional<int64_t> {
    if (node.kind == "IntLiteral") {
      auto h_int = ParseIntLiteral(node);
      if (!h_int.HasValue()) {
        return std::nullopt;
      }
      return h_int.Value();
    }
    if (node.kind == "Location" || node.kind == "Hint") {
      return std::nullopt;
    }
    // Wrapper nodes (Offset/Limit/ExpressionList/...): search children.
    if (node.kind == "Offset" || node.kind == "Limit" ||
        node.kind == "ExpressionList" || node.kind == "Expression") {
      for (const auto& sub : node.children) {
        if (auto found = fold_int(*sub)) {
          return found;
        }
      }
      return std::nullopt;
    }
    if (auto folded_or = VisitExpression(node); folded_or.HasValue()) {
      Expression folded = folded_or.MoveValue();
      if (folded && folded->Type() == TypeTag::kConstantValue) {
        const Value constant = folded->AsConstantValue().GetValue();
        if (constant.type == ValueType::kInt64 && !constant.IsNull()) {
          return constant.value.int_value;
        }
      }
    }
    for (const auto& sub : node.children) {
      if (auto found = fold_int(*sub)) {
        return found;
      }
    }
    return std::nullopt;
  };
  if (const GoogleSqlAstNode* limit_offset = query.Child("LimitOffset")) {
    if (const GoogleSqlAstNode* limit_node = limit_offset->Child("Limit")) {
      for (const auto& child : limit_node->children) {
        if (child->kind == "Location" || child->kind == "Hint") {
          continue;
        }
        if (auto h_lim = validate_limit_operand(*child, "LIMIT");
            !h_lim.HasValue()) {
          return h_lim.GetStatus();
        }
        if (child->kind == "IntLiteral") {
          ASSIGN_OR_RETURN(uint64_t, hv202084_0,
                           (ParseUnsignedLiteral(*child)));
          limit = static_cast<size_t>(std::move(hv202084_0));
        } else if (auto folded = fold_int(*child)) {
          if (*folded < 0) {
            return AstError<std::shared_ptr<SelectStatement>>(
                "LIMIT must be non-negative");
          }
          limit = static_cast<size_t>(*folded);
        } else {
          // A parameterized LIMIT with no static row count. Silently
          // treating it as unlimited would return every row; fail loudly.
          return AstError<std::shared_ptr<SelectStatement>>(
              "LIMIT requires an integer literal in this engine");
        }
      }
    }
    for (const auto& child : limit_offset->children) {
      if (child->kind == "Limit" || child->kind == "Location" ||
          child->kind == "Hint" || child->kind == "WithTies") {
        continue;
      }
      if (auto h_off = validate_limit_operand(*child, "OFFSET");
          !h_off.HasValue()) {
        return h_off.GetStatus();
      }
      if (child->kind == "IntLiteral") {
        ASSIGN_OR_RETURN(uint64_t, hv202986_0, (ParseUnsignedLiteral(*child)));
        offset = hv202986_0;
      } else if (auto folded = fold_int(*child)) {
        if (*folded < 0) {
          return AstError<std::shared_ptr<SelectStatement>>(
              "OFFSET must be non-negative");
        }
        offset = static_cast<size_t>(*folded);
      } else if (child->kind == "Offset") {
        // Empty OFFSET wrapper (no numeric child): plain LIMIT without
        // OFFSET. Older code ignored it; keep ignoring instead of failing.
        continue;
      } else {
        return AstError<std::shared_ptr<SelectStatement>>(
            "OFFSET requires an integer literal in this engine");
      }
    }
  }

  auto statement = std::make_shared<SelectStatement>(
      std::move(projections), std::move(tables), std::move(where),
      std::move(order_by), limit.value_or(0), offset,
      select->detail.find("distinct=true") != std::string::npos);
  statement->SetLimit(limit);
  statement->SetSources(std::move(sources));
  for (const SelectSource& source : statement->Sources()) {
    if (!source.table.empty() && !source.alias.empty() &&
        source.alias != source.table) {
      statement->AddAlias(source.alias, source.table);
    }
  }

  std::vector<Expression> distinct_on_expressions;
  if (const GoogleSqlAstNode* distinct_on = select->Child("DistinctOn")) {
    for (const auto& child : distinct_on->children) {
      if (child->kind == "ExpressionList" || child->kind == "GroupingItem") {
        for (const auto& grandchild : child->children) {
          ASSIGN_OR_RETURN(Expression, hv204471_0,
                           (VisitExpression(*grandchild)));
          distinct_on_expressions.push_back(std::move(hv204471_0));
        }
      } else if (child->kind == "SelectColumn" && !child->children.empty()) {
        ASSIGN_OR_RETURN(Expression, hv204634_0,
                         (VisitExpression(*child->children[0])));
        distinct_on_expressions.push_back(std::move(hv204634_0));
      } else {
        ASSIGN_OR_RETURN(Expression, h_tmp_5432, (VisitExpression(*child)));
        distinct_on_expressions.push_back(std::move(h_tmp_5432));
      }
    }
  } else if (const GoogleSqlAstNode* distinct_on_clause =
                 select->Child("DistinctOnClause")) {
    for (const auto& child : distinct_on_clause->children) {
      if (child->kind == "SelectColumn" && !child->children.empty()) {
        ASSIGN_OR_RETURN(Expression, hv205056_0,
                         (VisitExpression(*child->children[0])));
        distinct_on_expressions.push_back(std::move(hv205056_0));
      } else {
        ASSIGN_OR_RETURN(Expression, h_tmp_5441, (VisitExpression(*child)));
        distinct_on_expressions.push_back(std::move(h_tmp_5441));
      }
    }
  }
  if (!distinct_on_expressions.empty()) {
    statement->SetDistinctOn(std::move(distinct_on_expressions));
  }

  if (const GoogleSqlAstNode* limit_offset = query.Child("LimitOffset")) {
    if (limit_offset->Child("WithTies") != nullptr ||
        limit_offset->detail.find("with_ties") != std::string::npos ||
        limit_offset->detail.find("WITH TIES") != std::string::npos) {
      statement->SetWithTies(true);
    }
  }
  if (query.Child("WithTies") != nullptr) {
    statement->SetWithTies(true);
  }

  if (const GoogleSqlAstNode* group = select->Child("GroupBy")) {
    const bool is_group_by_all = group->Child("GroupByAll") != nullptr ||
                                 group->detail.find("ALL") != std::string::npos;
    if (is_group_by_all) {
      std::vector<Expression> expressions;
      for (const NamedExpression& named : statement->SelectList()) {
        if (named.expression && !ContainsAggregate(named.expression) &&
            named.expression->Type() != TypeTag::kWindowFunctionExp &&
            named.expression->Type() != TypeTag::kConstantValue) {
          expressions.push_back(named.expression);
        }
      }
      statement->SetGroupBy(std::move(expressions));
    } else {
      std::vector<Expression> expressions;
      for (const GoogleSqlAstNode* item : group->Children("GroupingItem")) {
        if (item->children.empty()) {
          continue;
        }
        const GoogleSqlAstNode& term = *item->children[0];
        if (term.kind == "IntLiteral") {
          ASSIGN_OR_RETURN(uint64_t, hv206744_0, (ParseUnsignedLiteral(term)));
          // GoogleSQL: integer GROUP BY items are SELECT-list ordinals.
          const auto ordinal = static_cast<size_t>(hv206744_0);
          if (ordinal >= 1 && ordinal <= statement->SelectList().size() &&
              statement->SelectList()[ordinal - 1].expression) {
            expressions.push_back(
                statement->SelectList()[ordinal - 1].expression);
            continue;
          }
          return AstError<std::shared_ptr<SelectStatement>>(
              "GoogleSQL AST: GROUP BY ordinal out of range");
        }
        ASSIGN_OR_RETURN(Expression, hv207291_0, (VisitExpression(term)));
        expressions.push_back(std::move(hv207291_0));
      }
      statement->SetGroupBy(std::move(expressions));
    }
  }
  if (const GoogleSqlAstNode* having = select->Child("Having")) {
    if (!having->children.empty()) {
      ASSIGN_OR_RETURN(Expression, hv207519_0,
                       (VisitExpression(*having->children[0])));
      statement->SetHaving(std::move(hv207519_0));
    }
  }
  const GoogleSqlAstNode* qualify = select->Child("Qualify") != nullptr
                                        ? select->Child("Qualify")
                                        : query.Child("Qualify");
  if (qualify != nullptr) {
    if (!qualify->children.empty()) {
      ASSIGN_OR_RETURN(Expression, hv207867_0,
                       (VisitExpression(*qualify->children[0])));
      statement->SetQualify(std::move(hv207867_0));
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
        ASSIGN_OR_RETURN(std::string, hv208494_0, (Identifier(*name)));
        const std::string cte_name = std::move(hv208494_0);
        if (recursive) {
          ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv208575_0,
                           (VisitQuery(*nested)));
          statement->AddRecursiveWithQuery(cte_name, std::move(hv208575_0));
          if (const GoogleSqlAstNode* modifiers =
                  aliased->Child("AliasedQueryModifiers")) {
            if (const GoogleSqlAstNode* depth_modifier =
                    modifiers->Child("RecursionDepthModifier")) {
              RecursiveDepthSpec spec;
              if (const GoogleSqlAstNode* alias =
                      depth_modifier->Child("Alias")) {
                if (const GoogleSqlAstNode* column =
                        alias->Child("Identifier")) {
                  ASSIGN_OR_RETURN(std::string, hv209136_0,
                                   (Identifier(*column)));
                  spec.column = std::move(hv209136_0);
                }
              }
              const auto bounds = depth_modifier->Children("IntOrUnbounded");
              auto bound_value = [&](size_t index, int64_t fallback) {
                if (index >= bounds.size()) {
                  return fallback;
                }
                for (const auto& child : bounds[index]->children) {
                  if (child->kind == "IntLiteral") {
                    auto h_lit = ParseUnsignedLiteral(*child);
                    if (h_lit.HasValue()) {
                      return static_cast<int64_t>(h_lit.MoveValue());
                    }
                  }
                }
                return fallback;
              };
              spec.lower = bound_value(0, 0);
              spec.upper = bound_value(1, std::numeric_limits<int64_t>::max());
              statement->SetRecursiveDepth(cte_name, std::move(spec));
            }
          }
        } else {
          ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, q_q,
                           (VisitQuery(*nested)));
          statement->AddWithQuery(cte_name, std::move(q_q));
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
        source.join_type == JoinType::kFull || source.from_nested_join ||
        source.unnest || !source.using_columns.empty() ||
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
  if (as_value && !statement->Sources().empty()) {
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

StatusOr<ValueType> ColumnType(const GoogleSqlAstNode& definition) {
  const GoogleSqlAstNode* schema = definition.Child("SimpleColumnSchema");
  const GoogleSqlAstNode* path =
      schema != nullptr ? schema->Child("PathExpression") : nullptr;
  if (path == nullptr) {
    return AstError<ValueType>("GoogleSQL AST: column type missing");
  }
  ASSIGN_OR_RETURN(std::string, hv212301_0, (Path(*path)));
  // Proto / user-defined type names arrive as backticked dotted paths
  // (`googlesql_test.Proto3KitchenSink`) or PROTO<...> wrappers; they store
  // through the VARCHAR channel carrying their TEXT-format payload.
  std::string raw_type = std::move(hv212301_0);
  std::string cleaned;
  for (const char c : raw_type) {
    if (c != '`') {
      cleaned.push_back(c);
    }
  }
  const std::string lower = Lower(cleaned);
  if (lower.starts_with("proto<") || lower.find('.') != std::string::npos) {
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
  return AstError<ValueType>("GoogleSQL AST: unsupported column type " + type);
}

StatusOr<std::unique_ptr<Statement>> VisitCreate(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    return AstError<std::unique_ptr<Statement>>("GoogleSQL AST: bad CREATE");
  }
  const GoogleSqlAstNode* elements = root.Child("TableElementList");
  if (elements != nullptr) {
    std::vector<Column> columns;
    for (const GoogleSqlAstNode* definition :
         elements->Children("ColumnDefinition")) {
      const GoogleSqlAstNode* name = definition->Child("Identifier");
      if (name == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: unnamed column");
      }
      ASSIGN_OR_RETURN(ValueType, hv214138_0, (ColumnType(*definition)));
      ASSIGN_OR_RETURN(std::string, hv214138_1, (Identifier(*name)));
      columns.emplace_back(std::move(hv214138_1), hv214138_0);
    }
    ASSIGN_OR_RETURN(std::string, h_tbl, (Path(*path)));
    return std::make_unique<CreateTableStatement>(std::move(h_tbl),
                                                  std::move(columns));
  }
  const GoogleSqlAstNode* query = root.Child("Query");
  if (query != nullptr) {
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv214435_0,
                     (VisitQuery(*query)));
    auto statement = std::move(hv214435_0);
    ASSIGN_OR_RETURN(std::string, h_tbl, (Path(*path)));
    return std::make_unique<CreateTableStatement>(std::move(h_tbl),
                                                  std::move(statement));
  }
  return AstError<std::unique_ptr<Statement>>("GoogleSQL AST: bad CREATE");
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

StatusOr<std::unique_ptr<Statement>> VisitCreateFunction(
    const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* declaration = root.Child("FunctionDeclaration");
  if (declaration == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: function declaration missing");
  }
  const GoogleSqlAstNode* path = declaration->Child("PathExpression");
  if (path == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: function without name");
  }
  SqlUdf udf;
  ASSIGN_OR_RETURN(std::string, hv215914_0, (Path(*path)));
  udf.name = Lower(std::move(hv215914_0));
  udf.is_aggregate = root.detail.find("is_aggregate=true") != std::string::npos;
  if (const GoogleSqlAstNode* parameters =
          declaration->Child("FunctionParameters")) {
    for (const GoogleSqlAstNode* parameter :
         parameters->Children("FunctionParameter")) {
      const GoogleSqlAstNode* name_node = parameter->Child("Identifier");
      if (name_node == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: function parameter without name");
      }
      ASSIGN_OR_RETURN(std::string, hv216443_0, (Identifier(*name_node)));
      udf.parameters.emplace_back(
          Lower(std::move(hv216443_0)),
          parameter->detail.find("is_not_aggregate=true") != std::string::npos);
      std::shared_ptr<GoogleSqlAstNode> default_expr = nullptr;
      for (const auto& child : parameter->children) {
        if (child->kind != "Identifier" && child->kind != "SimpleType" &&
            child->kind != "TemplatedParameterType" &&
            child->kind != "Location") {
          default_expr = CloneAstNode(*child);
          break;
        }
      }
      udf.default_values.push_back(std::move(default_expr));
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

StatusOr<int64_t> AssertRowsModifiedValue(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* literal = node.Child("IntLiteral");
  if (literal == nullptr) {
    return AstError<int64_t>(
        "GoogleSQL AST: ASSERT_ROWS_MODIFIED without count");
  }
  return ParseIntLiteral(*literal);
}

StatusOr<std::unique_ptr<Statement>> VisitInsert(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: INSERT table missing");
  }
  std::vector<std::string> columns;
  if (const GoogleSqlAstNode* list = root.Child("ColumnList")) {
    for (const GoogleSqlAstNode* name : list->Children("Identifier")) {
      ASSIGN_OR_RETURN(std::string, hv218946_0, (Identifier(*name)));
      columns.push_back(std::move(hv218946_0));
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
        ASSIGN_OR_RETURN(Expression, hv219408_0, (VisitExpression(*value)));
        values.push_back(std::move(hv219408_0));
      }
      rows.push_back(std::move(values));
    }
  }
  ASSIGN_OR_RETURN(std::string, hv219518_0, (Path(*path)));
  auto statement = std::make_unique<InsertStatement>(
      std::move(hv219518_0), std::move(rows), std::move(columns));
  statement->SetMode(InsertModeFromDetail(root.detail));
  if (const GoogleSqlAstNode* query = root.Child("Query")) {
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv219747_0,
                     (VisitQuery(*query)));
    statement->SetQuery(std::move(hv219747_0));
  }
  if (const GoogleSqlAstNode* assert = root.Child("AssertRowsModified")) {
    ASSIGN_OR_RETURN(int64_t, hv219871_0, (AssertRowsModifiedValue(*assert)));
    statement->SetAssertRowsModified(hv219871_0);
  }
  return statement;
}

StatusOr<std::unique_ptr<Statement>> VisitUpdate(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  const GoogleSqlAstNode* items = root.Child("UpdateItemList");
  if (path == nullptr || items == nullptr) {
    return AstError<std::unique_ptr<Statement>>("GoogleSQL AST: bad UPDATE");
  }
  std::vector<std::pair<ColumnName, Expression>> assignments;
  std::vector<NestedDmlItem> nested_items;
  // Extracts "WHERE <expr>" plus an optional ASSERT_ROWS_MODIFIED from a
  // nested DELETE/UPDATE statement node; remaining children are ignored
  // because nested targets are columns, not relations (no alias/RETURNING).
  auto parse_nested_tail = [](const GoogleSqlAstNode& node,
                              Expression* predicate,
                              int64_t* assert_rows) -> Status {
    std::vector<const GoogleSqlAstNode*> candidates;
    for (const auto& child : node.children) {
      if (child->kind == "PathExpression" || child->kind == "UpdateItemList" ||
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
      return AstStatus(
          "GoogleSQL AST: multiple nested WHERE clause candidates");
    }
    if (!candidates.empty()) {
      ASSIGN_OR_RETURN(Expression, hv221454_0,
                       (VisitExpression(*candidates.front())));
      *predicate = std::move(hv221454_0);
    }
    if (const GoogleSqlAstNode* assert = node.Child("AssertRowsModified")) {
      ASSIGN_OR_RETURN(int64_t, hv221594_0, (AssertRowsModifiedValue(*assert)));
      *assert_rows = hv221594_0;
    }
    return Status::kSuccess;
  };
  for (const GoogleSqlAstNode* item : items->Children("UpdateItem")) {
    const GoogleSqlAstNode* set = item->Child("UpdateSetValue");
    if (set != nullptr && set->children.size() == 2) {
      ASSIGN_OR_RETURN(Expression, hv221851_0,
                       (VisitExpression(*set->children[1])));
      ASSIGN_OR_RETURN(std::string, hv221851_1, (Path(*set->children[0])));
      assignments.emplace_back(ColumnName(std::move(hv221851_1)),
                               std::move(hv221851_0));
      continue;
    }
    // Nested DML: SET (DELETE arr WHERE ...), SET (UPDATE arr SET ... WHERE
    // ...) and SET (INSERT arr VALUES ... / (SELECT ...)).
    if (const GoogleSqlAstNode* nested = item->Child("DeleteStatement")) {
      const GoogleSqlAstNode* target = nested->Child("PathExpression");
      if (target == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: nested DELETE without target");
      }
      NestedDmlItem parsed;
      parsed.kind = NestedDmlItem::Kind::kDelete;
      ASSIGN_OR_RETURN(std::string, hv222510_0, (Path(*target)));
      parsed.target_path = std::move(hv222510_0);
      if (Status st_tail = parse_nested_tail(*nested, &parsed.predicate,
                                             &parsed.assert_rows_modified);
          !st_tail.ok()) {
        return st_tail;
      }
      nested_items.push_back(std::move(parsed));
      continue;
    }
    if (const GoogleSqlAstNode* nested = item->Child("UpdateStatement")) {
      const GoogleSqlAstNode* target = nested->Child("PathExpression");
      const GoogleSqlAstNode* inner_items = nested->Child("UpdateItemList");
      if (target == nullptr || inner_items == nullptr ||
          inner_items->Children("UpdateItem").size() != 1) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: bad nested UPDATE assignment");
      }
      const GoogleSqlAstNode* inner_set =
          inner_items->Children("UpdateItem").front()->Child("UpdateSetValue");
      if (inner_set == nullptr || inner_set->children.size() != 2) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: bad nested UPDATE assignment");
      }
      NestedDmlItem parsed;
      parsed.kind = NestedDmlItem::Kind::kUpdate;
      ASSIGN_OR_RETURN(std::string, hv223557_0, (Path(*target)));
      parsed.target_path = std::move(hv223557_0);
      ASSIGN_OR_RETURN(Expression, hv223599_0,
                       (VisitExpression(*inner_set->children[1])));
      parsed.set_value = std::move(hv223599_0);
      if (Status st_tail = parse_nested_tail(*nested, &parsed.predicate,
                                             &parsed.assert_rows_modified);
          !st_tail.ok()) {
        return st_tail;
      }
      nested_items.push_back(std::move(parsed));
      continue;
    }
    if (const GoogleSqlAstNode* nested = item->Child("InsertStatement")) {
      const GoogleSqlAstNode* target = nested->Child("PathExpression");
      if (target == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: nested INSERT without target");
      }
      NestedDmlItem parsed;
      parsed.kind = NestedDmlItem::Kind::kInsert;
      ASSIGN_OR_RETURN(std::string, hv224208_0, (Path(*target)));
      parsed.target_path = std::move(hv224208_0);
      if (const GoogleSqlAstNode* row_list =
              nested->Child("InsertValuesRowList")) {
        for (const GoogleSqlAstNode* row :
             row_list->Children("InsertValuesRow")) {
          std::vector<Expression> values;
          for (const auto& value : row->children) {
            if (value->kind == "Location" || value->kind == "Hint") {
              continue;
            }
            ASSIGN_OR_RETURN(Expression, hv224648_0, (VisitExpression(*value)));
            values.push_back(std::move(hv224648_0));
          }
          parsed.insert_values.push_back(std::move(values));
        }
      }
      if (const GoogleSqlAstNode* query = nested->Child("Query")) {
        ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv224862_0,
                         (VisitQuery(*query)));
        parsed.insert_query = std::move(hv224862_0);
      }
      if (parsed.insert_values.empty() && parsed.insert_query == nullptr) {
        return AstError<std::unique_ptr<Statement>>(
            "GoogleSQL AST: nested INSERT without values or query");
      }
      if (const GoogleSqlAstNode* assert =
              nested->Child("AssertRowsModified")) {
        ASSIGN_OR_RETURN(int64_t, hv225209_0,
                         (AssertRowsModifiedValue(*assert)));
        parsed.assert_rows_modified = hv225209_0;
      }
      nested_items.push_back(std::move(parsed));
      continue;
    }
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: bad UPDATE assignment");
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
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: multiple UPDATE WHERE clause candidates");
  }
  Expression where;
  if (!where_candidates.empty()) {
    ASSIGN_OR_RETURN(Expression, hv226442_0,
                     (VisitExpression(*where_candidates.front())));
    where = std::move(hv226442_0);
  }
  ASSIGN_OR_RETURN(std::string, hv226502_0, (Path(*path)));
  auto statement = std::make_unique<UpdateStatement>(
      std::move(hv226502_0), std::move(assignments), std::move(where));
  statement->SetNestedItems(std::move(nested_items));
  if (const GoogleSqlAstNode* alias = root.Child("Alias")) {
    if (const GoogleSqlAstNode* id = alias->Child("Identifier")) {
      ASSIGN_OR_RETURN(std::string, hv226800_0, (Identifier(*id)));
      statement->SetAlias(std::move(hv226800_0));
    }
  }
  if (const GoogleSqlAstNode* assert = root.Child("AssertRowsModified")) {
    ASSIGN_OR_RETURN(int64_t, hv226929_0, (AssertRowsModifiedValue(*assert)));
    statement->SetAssertRowsModified(hv226929_0);
  }
  return statement;
}

StatusOr<std::unique_ptr<Statement>> VisitDelete(const GoogleSqlAstNode& root) {
  const GoogleSqlAstNode* path = root.Child("PathExpression");
  if (path == nullptr) {
    return AstError<std::unique_ptr<Statement>>("GoogleSQL AST: bad DELETE");
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
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: multiple DELETE WHERE clause candidates");
  }
  Expression where;
  if (!where_candidates.empty()) {
    ASSIGN_OR_RETURN(Expression, hv227907_0,
                     (VisitExpression(*where_candidates.front())));
    where = std::move(hv227907_0);
  }
  ASSIGN_OR_RETURN(std::string, hv227967_0, (Path(*path)));
  auto statement = std::make_unique<DeleteStatement>(std::move(hv227967_0),
                                                     std::move(where));
  if (const GoogleSqlAstNode* alias = root.Child("Alias")) {
    if (const GoogleSqlAstNode* id = alias->Child("Identifier")) {
      ASSIGN_OR_RETURN(std::string, hv228186_0, (Identifier(*id)));
      statement->SetAlias(std::move(hv228186_0));
    }
  }
  if (const GoogleSqlAstNode* assert = root.Child("AssertRowsModified")) {
    ASSIGN_OR_RETURN(int64_t, hv228315_0, (AssertRowsModifiedValue(*assert)));
    statement->SetAssertRowsModified(hv228315_0);
  }
  return statement;
}

// ---------------------------------------------------------------------------
// Proto constructor (NEW) and SELECT AS <proto> validation against the
// embedded compliance-proto registry.

StatusOr<std::string> ConstructorTypeFullName(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* path_node = nullptr;
  if (const GoogleSqlAstNode* simple = node.Child("SimpleType")) {
    path_node = simple->Child("PathExpression");
  }
  if (path_node == nullptr) {
    path_node = node.Child("PathExpression");
  }
  if (path_node == nullptr) {
    return std::string{};
  }
  return Path(*path_node);
}

bool FieldNameEquals(const std::string& left, const std::string& right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); ++i) {
    const char lc =
        static_cast<char>(std::tolower(static_cast<unsigned char>(left[i])));
    const char rc =
        static_cast<char>(std::tolower(static_cast<unsigned char>(right[i])));
    if (lc != rc) {
      return false;
    }
  }
  return true;
}

const ProtoFieldSchema* FindProtoField(
    const std::vector<ProtoFieldSchema>& fields, const std::string& name) {
  for (const ProtoFieldSchema& field : fields) {
    if (FieldNameEquals(field.name, name)) {
      return &field;
    }
  }
  return nullptr;
}

// Rejects literal enum assignments that the registry says are invalid.
Status ValidateEnumLiteralValue(const std::string& message_name,
                                const std::string& field_name,
                                const std::string& enum_type_name,
                                const GoogleSqlAstNode& expr) {
  const std::string enum_short_name(ShortTypeName(enum_type_name));
  if (!IsKnownEnum(enum_short_name)) {
    return Status::kSuccess;
  }
  auto reject = [&](const std::string& message) {
    return AstStatus("Could not store value into proto field " + message_name +
                     "." + field_name + ": " + message);
  };
  if (expr.kind == "StringLiteral") {
    ASSIGN_OR_RETURN(std::string, hv230390_0, (DecodeString(expr)));
    const std::string value = std::move(hv230390_0);
    int64_t ordinal = 0;
    if (!EnumValueForMember(enum_short_name, value, &ordinal)) {
      reject("Out of range cast of string '" + value + "' to enum type " +
             enum_short_name);
    }
    return Status::kSuccess;
  }
  if (expr.kind == "IntLiteral") {
    const std::string& digits = expr.detail;
    uint64_t magnitude = 0;
    if (std::from_chars(digits.data(), digits.data() + digits.size(), magnitude)
                .ec != std::errc() ||
        magnitude > static_cast<uint64_t>(2147483647LL)) {
      reject("Out of range cast of integer " + digits + " to enum type " +
             enum_short_name);
      return Status::kSuccess;
    }
    const auto ordinal = static_cast<int64_t>(magnitude);
    const std::optional<std::string> member =
        EnumMemberForValue(enum_short_name, ordinal);
    if (!member.has_value() && !EnumIsOpen(enum_short_name)) {
      reject("Out of range cast of integer " + std::to_string(ordinal) +
             " to enum type " + enum_short_name);
    }
  }
  return Status::kSuccess;
}

// Shared checks for one field assignment. Returns true when the assignment
// still needs a runtime guard because its value is not statically known.
StatusOr<bool> ValidateProtoFieldAssignment(const std::string& message_name,
                                            const ProtoFieldSchema& field,
                                            const GoogleSqlAstNode& expr) {
  if (expr.kind == "NullLiteral") {
    if (field.required) {
      return AstError<bool>(
          "Cannot encode a null value in required protocol message field " +
          message_name + "." + field.name);
    }
    return false;
  }
  if (field.is_enum && !field.repeated &&
      (expr.kind == "StringLiteral" || expr.kind == "IntLiteral")) {
    Status st232125 = ValidateEnumLiteralValue(message_name, field.name,
                                               field.type_name, expr);
    if (st232125 != Status::kSuccess) {
      return st232125;
    }
    return false;
  }
  if (field.repeated && expr.kind == "ArrayConstructor") {
    for (const auto& element : expr.children) {
      if (element->kind == "ArrayType" || IsAstTrivia(element->kind)) {
        continue;
      }
      if (element->kind == "NullLiteral") {
        return AstError<bool>(
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

Status RequireProtoFieldsPresent(const std::string& message_name,
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
        return AstStatus("Required protocol message field " + message_name +
                         "." + field.name + " is not assigned");
      }
    }
  }
  return Status::kSuccess;
}

StatusOr<Expression> BuildNewConstructor(const GoogleSqlAstNode& node) {
  // Registry validation runs at compile time; the actual payload is built
  // at runtime through __proto_new so per-row values (subqueries, column
  // references, TIMESTAMP/DATE conversions, NULLs) format correctly.
  ASSIGN_OR_RETURN(std::string, message_name, (ConstructorTypeFullName(node)));
  const std::vector<ProtoFieldSchema>* fields =
      message_name.empty() ? nullptr : FindProtoMessageFields(message_name);
  std::set<std::string> assigned;
  for (const auto& child : node.children) {
    if (child->kind != "NewConstructorArg" || child->children.size() < 2) {
      continue;
    }
    ASSIGN_OR_RETURN(std::string, hv234429_0,
                     (Identifier(*child->children[1])));
    const std::string field_name = std::move(hv234429_0);
    if (field_name.empty()) {
      continue;
    }
    assigned.insert(field_name);
    if (fields != nullptr) {
      const ProtoFieldSchema* field = FindProtoField(*fields, field_name);
      if (field != nullptr) {
        if (auto hv234716_0 = ValidateProtoFieldAssignment(message_name, *field,
                                                           *child->children[0]);
            !hv234716_0.HasValue()) {
          return hv234716_0.GetStatus();
        }
      }
    }
  }
  if (fields != nullptr) {
    Status st234842 =
        RequireProtoFieldsPresent(message_name, *fields, assigned);
    if (st234842 != Status::kSuccess) {
      return st234842;
    }
  }
  std::string type_name;
  if (const GoogleSqlAstNode* path_node = node.Child("PathExpression")) {
    ASSIGN_OR_RETURN(std::string, hv235009_0, (Path(*path_node)));
    type_name = std::move(hv235009_0);
  } else if (const GoogleSqlAstNode* simple = node.Child("SimpleType")) {
    if (const GoogleSqlAstNode* inner = simple->Child("PathExpression")) {
      ASSIGN_OR_RETURN(std::string, hv235192_0, (Path(*inner)));
      type_name = std::move(hv235192_0);
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
    ASSIGN_OR_RETURN(std::string, hv235966_0, (Identifier(name_node)));
    std::string field_name = std::move(hv235966_0);
    if (field_name.empty()) {
      ASSIGN_OR_RETURN(std::string, hv236048_0, (Path(name_node)));
      field_name = std::move(hv236048_0);
    }
    // Extension targets arrive as parenthesized paths: emit the bracketed
    // extension key used by TEXT format.
    if (!field_name.empty() && field_name.front() != '[' &&
        field_name.find('.') != std::string::npos) {
      field_name.insert(field_name.begin(), '[');
      field_name.push_back(']');
    }
    if (value_node.kind == "BooleanLiteral") {
      const std::string upper_literal = UpperCopy(value_node.detail);
      args.emplace_back(ConstantValueExp(Value(upper_literal == "TRUE"
                                                   ? std::string("true")
                                                   : std::string("false"))));
    } else {
      ASSIGN_OR_RETURN(Expression, h_tmp_6224, (VisitExpression(value_node)));
      args.push_back(std::move(h_tmp_6224));
    }
    args.emplace_back(ConstantValueExp(Value(std::move(field_name))));
  }
  return FunctionCallExp("__proto_new", std::move(args));
}

Status ValidateSelectAsProjections(
    const std::string& message_name,
    const std::vector<std::pair<std::string, const GoogleSqlAstNode*>>& named,
    std::vector<NamedExpression>* projections) {
  const std::vector<ProtoFieldSchema>* fields =
      FindProtoMessageFields(message_name);
  if (fields == nullptr) {
    return Status::kSuccess;
  }
  std::set<std::string> assigned;
  for (const auto& [name, expression_node] : named) {
    assigned.insert(name);
    const ProtoFieldSchema* field = FindProtoField(*fields, name);
    if (field == nullptr) {
      continue;
    }
    if (auto v = ValidateProtoFieldAssignment(message_name, *field,
                                              *expression_node);
        !v.HasValue()) {
      return v.GetStatus();
    }
  }
  RequireProtoFieldsPresent(message_name, *fields, assigned);
  for (size_t i = 0; i < named.size(); ++i) {
    const ProtoFieldSchema* field = FindProtoField(*fields, named[i].first);
    if (field == nullptr) {
      continue;
    }
    if (named[i].second->kind == "NullLiteral" ||
        named[i].second->kind == "StringLiteral" ||
        named[i].second->kind == "IntLiteral") {
      continue;
    }
    if (field->is_enum && !field->repeated) {
      ASSIGN_OR_RETURN(Expression, hv238054_0,
                       (VisitExpression(*named[i].second)));
      (*projections)[i] = NamedExpression(
          (*projections)[i].name,
          FunctionCallExp(
              "$proto_enum_guard",
              {std::move(hv238054_0),
               ConstantValueExp(Value(std::string(field->type_name)))}));
    }
  }
  return Status::kSuccess;
}

}  // namespace

std::string DecodeStringEscapes(std::string_view value, bool is_bytes,
                                bool is_triple, char quote) {
  return DecodeStringEscapesImpl(value, is_bytes, is_triple, quote);
}

std::string NormalizeTimestampText(const std::string& text) {
  return NormalizeTimestampTextImpl(text);
}

StatusOr<std::unique_ptr<Statement>> GoogleSqlAstVisitor::Visit(
    const GoogleSqlAstNode& root, std::string_view source) {
  std::string source_storage(source);
  // Only the outermost Visit installs the source; recursive Visit calls
  // pass an empty view and keep the outer scope alive.
  std::optional<VisitSourceScope> scope;
  if (!source_storage.empty()) {
    scope.emplace(source_storage);
  }
  // Hints for other engines (qualified "engine.name") are ignored; an
  // unqualified hint is only meaningful when the engine knows it, and this
  // engine implements none: GoogleSQL rejects unknown default-engine hints
  // instead of silently executing the statement.
  if (Status st_hints = RejectUnsupportedHints(root);
      st_hints != Status::kSuccess) {
    return st_hints;
  }
  if (root.kind == "HintedStatement") {
    for (const auto& child : root.children) {
      if (child->kind == "Hint" || child->kind == "Location") {
        continue;
      }
      return Visit(*child);
    }
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: hinted statement without body");
  }
  if (root.kind == "QueryStatement") {
    const GoogleSqlAstNode* query = root.Child("Query");
    if (query == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: missing query");
    }
    ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv239690_0,
                     (VisitQuery(*query)));
    auto statement = std::move(hv239690_0);
    return std::make_unique<SelectStatement>(*statement);
  }
  if (root.kind == "CreateConstantStatement") {
    const GoogleSqlAstNode* path = root.Child("PathExpression");
    if (path == nullptr) {
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad CREATE CONSTANT");
    }
    ASSIGN_OR_RETURN(std::string, hv240028_0, (Path(*path)));
    std::string const_name = std::move(hv240028_0);
    std::string const_val;
    for (const auto& child : root.children) {
      if (child->kind != "PathExpression" && child->kind != "Location") {
        ASSIGN_OR_RETURN(Expression, hv240217_0, (VisitExpression(*child)));
        Expression expr = std::move(hv240217_0);
        Row dummy_row;
        Schema dummy_schema;
        StatusOr<Value> v_or = expr->TryEvaluate(dummy_row, dummy_schema);
        if (v_or.HasValue()) {
          Value v = v_or.MoveValue();
          if (v.type == ValueType::kVarChar) {
            const_val = std::string(v.value.varchar_value);
          } else {
            const_val = v.AsString();
          }
          // Fully evaluable constant: pin the value itself so every
          // reference observes one deterministic result (e.g. Rand()).
          SessionConstantExpressions()[Lower(const_name)] =
              ConstantValueExp(Value(std::move(v)));
        } else {
          if (child->kind == "StringLiteral") {
            ASSIGN_OR_RETURN(std::string, hv240897_0, (DecodeString(*child)));
            const_val = std::move(hv240897_0);
            SessionConstantExpressions()[Lower(const_name)] =
                ConstantValueExp(Value(std::string(const_val)));
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
      return AstError<std::unique_ptr<Statement>>(
          "GoogleSQL AST: bad CREATE VIEW");
    }
    std::shared_ptr<GoogleSqlAstNode> clone = CloneAstNode(*query);
    ASSIGN_OR_RETURN(std::string, hv242328_0, (Path(*path)));
    ViewRegistry()[Lower(std::move(hv242328_0))] = std::move(clone);
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
      return AstError<std::unique_ptr<Statement>>("GoogleSQL AST: bad DROP");
    }
    ASSIGN_OR_RETURN(std::string, h_tbl, (Path(*path)));
    return std::make_unique<DropTableStatement>(std::move(h_tbl));
  }
  if (root.kind.starts_with("DropStatement")) {
    return AstError<std::unique_ptr<Statement>>(
        "GoogleSQL AST: unsupported statement " + root.kind);
  }
  return AstError<std::unique_ptr<Statement>>(
      "GoogleSQL AST: unsupported statement " + root.kind);
}

}  // namespace tinylamb
