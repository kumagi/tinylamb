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
#include "common/platform/time_format.hpp"
#include "common/platform/time_zone.hpp"
#include "common/set_operation.hpp"
#include "common/string_util.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/json_doc.hpp"
#include "expression/named_expression.hpp"
#include "expression/proto_schema.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/sql_udf.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor_internal.hpp"
#include "query/statement.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

using namespace ast_visitor_detail;

namespace ast_visitor_detail {

thread_local std::vector<UdfExpansionFrame> t_udf_frames;
thread_local std::vector<std::unordered_set<std::string>> t_udf_bound_masks;
thread_local std::unordered_map<std::string, NamedWindowParts> t_named_windows;

namespace {

StatusOr<Expression> ExpandUdfCall(const std::string& name,
                                   std::vector<Expression> arguments);
void RecoverNullTreatment(const GoogleSqlAstNode& call, bool* respect_nulls,
                          bool* ignore_nulls);
StatusOr<std::string> InferArrayElementSqlType(const GoogleSqlAstNode& node);

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
  // Numeric UTC offsets share one strict parser with CAST/recovery: the old
  // sscanf variants accepted out-of-range hh/mm ("UTC+530" became +530h) and
  // long digit strings could overflow signed int. Failures keep the caller's
  // default rather than producing a bogus offset.
  if (tz_str.starts_with("UTC+") || tz_str.starts_with("UTC-") ||
      tz_str.starts_with("GMT+") || tz_str.starts_with("GMT-") ||
      tz_str[0] == '+' || tz_str[0] == '-') {
    auto parsed = TryTimeZoneOffsetAtLocalTime(
        tz_str, Y < 1970 ? 1970 : Y, static_cast<unsigned>(M),
        static_cast<unsigned>(D), h, m, s);
    return parsed.HasValue() ? parsed.Value() : default_offset;
  }
  std::string zone_name(tz_str);
  if (zone_name == "NZ-CHAT") {
    zone_name = "Pacific/Chatham";
  }
  int y = Y < 1970 ? 1970 : Y;
  const std::optional<int> offset = platform::TimeZoneOffsetAtLocalTime(
      zone_name, y, static_cast<unsigned>(M), static_cast<unsigned>(D), h, m,
      s);
  return offset.value_or(default_offset);
}

StatusOr<std::string> InferSubqueryArrayElementType(
    const GoogleSqlAstNode& query_node);
StatusOr<std::string> InferAggregateArrayElementType(
    const GoogleSqlAstNode& node);

// Deeply nested expressions (e.g. "1+1+1+..." chained 100k times) parse fine
// but would overflow the C++ stack during recursive visitation; a stack
// overflow is unrecoverable, so cap the visitation depth explicitly.
// VisitExpression carries a very large frame (every branch's locals live in
// one activation), and sanitizer builds inflate it further: with ASan the
// usable depth is only ~140 on an 8 MiB stack, so the cap must stay far
// below that to keep Debug/ASan runs from overflowing before the guard
// fires.
constexpr size_t kMaxExpressionDepth = 64;  // ~46% of the ~140 ASan limit

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
  // A failed guard never incremented the counter: decrementing anyway would
  // underflow the size_t to SIZE_MAX and make EVERY later parse on this
  // thread "exceed" the depth limit (observed as unrelated tests aborting
  // when several ran inside one process).
  ~ExpressionDepthGuard() {
    if (!failed_) {
      --ExpressionDepthCounter();
    }
  }
  ExpressionDepthGuard(const ExpressionDepthGuard&) = delete;
  ExpressionDepthGuard& operator=(const ExpressionDepthGuard&) = delete;
  ExpressionDepthGuard(ExpressionDepthGuard&&) = delete;
  ExpressionDepthGuard& operator=(ExpressionDepthGuard&&) = delete;

 private:
  bool failed_{false};
};

}  // namespace
// Dump-produced literals must be digits-only and in range; std::from_chars
// below enforces that without accepting signs or wrapping "-1".
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

// Window inner LIMIT is stored as size_t; a uint64 literal that does not
// fit (32-bit targets) must clamp to "unlimited", never truncate to a
// smaller limit.
size_t WindowLimitFromUint64(uint64_t v) {
  return static_cast<size_t>(std::min<uint64_t>(
      v, static_cast<uint64_t>(std::numeric_limits<size_t>::max())));
}

// Leading integer of an INTERVAL literal's raw text.  std::stoll's prefix
// match used to turn '3junk' into amount 3; strtoll with an end-pointer
// check plus a trailing-whitespace allowance keeps '+3' and ' 3 ' working
// while compound forms ('1-2') yield 0 — TryParse stays the validity
// authority and every Amount() consumer rejects compound units anyway.
int64_t ParseIntervalAmount(const std::string& raw) {
  const char* s = raw.c_str();
  char* end = nullptr;
  errno = 0;
  const long long v = std::strtoll(s, &end, 10);
  if (end == s || errno == ERANGE) {
    return 0;
  }
  while (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r' ||
         *end == '\f' || *end == '\v') {
    ++end;
  }
  return *end == '\0' ? static_cast<int64_t>(v) : 0;
}

namespace {
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

}  // namespace
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

namespace {

StatusOr<std::vector<std::string>> PathParts(const GoogleSqlAstNode& path) {
  std::vector<std::string> result;
  result.reserve(path.children.size());
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

}  // namespace

StatusOr<std::string> Path(const GoogleSqlAstNode& path) {
  ASSIGN_OR_RETURN(std::vector<std::string>, hv12514_0, (PathParts(path)));
  const std::vector<std::string> parts = std::move(hv12514_0);
  size_t joined_size = parts.size() - 1;
  for (const std::string& part : parts) {
    joined_size += part.size();
  }
  std::string result;
  result.reserve(joined_size);
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

namespace {

// Normalizes the WEEK(<weekday>) date_part spelling ("LAST_DAY(d,
// WEEK(MONDAY))", "EXTRACT(WEEK(SUNDAY) FROM d)") into the
// "week(<weekday>)" part token the evaluator consumes. Returns nullopt for
// anything else (a plain path, a quoted string, another function), so the
// generic handling applies. The bare weekday is a keyword in that position,
// not a column reference.
std::optional<std::string> WeekdayDatePartToken(const GoogleSqlAstNode& node) {
  if (node.kind != "FunctionCall" || node.children.empty()) {
    return std::nullopt;
  }
  auto name_or = Path(*node.children.front());
  if (!name_or.HasValue() || Lower(name_or.Value()) != "week" ||
      node.children.size() < 2) {
    return std::nullopt;
  }
  size_t argument_count = 0;
  const GoogleSqlAstNode* argument = nullptr;
  for (size_t i = 1; i < node.children.size(); ++i) {
    if (node.children[i]->kind != "Location") {
      ++argument_count;
      if (argument == nullptr) {
        argument = node.children[i].get();
      }
    }
  }
  if (argument_count != 1 || argument == nullptr) {
    return std::nullopt;
  }
  std::string text;
  if (argument->kind == "Identifier") {
    auto id_or = Identifier(*argument);
    if (!id_or.HasValue()) {
      return std::nullopt;
    }
    text = std::move(id_or.Value());
  } else if (argument->kind == "PathExpression") {
    auto path_or = Path(*argument);
    if (!path_or.HasValue() || path_or.Value().find('.') != std::string::npos) {
      return std::nullopt;
    }
    text = std::move(path_or.Value());
  } else if (argument->kind == "DateOrTimeUnit") {
    // The pinned parser lexes bare date-part keywords in argument position
    // as DateOrTimeUnit nodes ("LAST_DAY(d, MONTH)"); the generic visitor
    // folds them into string constants.
    if (const GoogleSqlAstNode* id = argument->Child("Identifier")) {
      auto id_or = Identifier(*id);
      if (!id_or.HasValue()) {
        return std::nullopt;
      }
      text = std::move(id_or.Value());
    } else {
      std::string unit = argument->detail;
      if (unit.starts_with("unit=")) {
        unit = unit.substr(5);
      }
      text = std::move(unit);
    }
  } else {
    return std::nullopt;
  }
  static constexpr std::array<std::string_view, 7> kWeekdays = {
      "sunday",   "monday", "tuesday", "wednesday",
      "thursday", "friday", "saturday"};
  const std::string lowered = Lower(std::move(text));
  for (std::string_view day : kWeekdays) {
    if (lowered == day) {
      return "week(" + std::string(day) + ")";
    }
  }
  return std::nullopt;
}

}  // namespace

namespace {
StatusOr<std::string> DecodeStringEscapesImpl(std::string_view value_view,
                                              bool is_bytes, bool is_triple,
                                              char quote) {
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
        // \xHH requires exactly two hex digits, same discipline as the
        // \u escape above; stoul's prefix match used to swallow a lone
        // hex digit plus a following non-hex character.
        if (value.size() - i - 1 < 2) {
          return AstError<std::string>("invalid hex escape");
        }
        const char* begin = value.data() + i + 1;
        uint32_t raw_byte = 0;
        const auto [end, error] =
            std::from_chars(begin, begin + 2, raw_byte, 16);
        if (error != std::errc{} || end != begin + 2) {
          return AstError<std::string>("invalid hex escape");
        }
        if (is_bytes || raw_byte <= 0x7F) {
          decoded.push_back(static_cast<char>(raw_byte));
        } else {
          decoded.push_back(static_cast<char>(0xC0 | ((raw_byte >> 6) & 0x1F)));
          decoded.push_back(static_cast<char>(0x80 | (raw_byte & 0x3F)));
        }
        i += 2;
      } else if (next == 'u' || next == 'U') {
        const size_t digits = next == 'u' ? 4 : 8;
        if (is_bytes || value.size() - i - 1 < digits) {
          return AstError<std::string>("invalid Unicode escape");
        }
        const char* begin = value.data() + i + 1;
        uint32_t cp = 0;
        const auto [end, error] =
            std::from_chars(begin, begin + digits, cp, 16);
        if (error != std::errc{} || end != begin + digits || cp > 0x10FFFF ||
            (cp >= 0xD800 && cp <= 0xDFFF)) {
          return AstError<std::string>("invalid Unicode scalar in escape");
        }
        if (cp <= 0x7F) {
          decoded.push_back(static_cast<char>(cp));
        } else if (cp <= 0x7FF) {
          decoded.push_back(static_cast<char>(0xC0 | (cp >> 6)));
          decoded.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else if (cp <= 0xFFFF) {
          decoded.push_back(static_cast<char>(0xE0 | (cp >> 12)));
          decoded.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
          decoded.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else {
          decoded.push_back(static_cast<char>(0xF0 | (cp >> 18)));
          decoded.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
          decoded.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
          decoded.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        }
        i += digits;
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

}  // namespace
StatusOr<std::string> DecodeSingleComponent(std::string_view value) {
  bool is_raw = false;
  bool is_bytes = false;
  if (!value.empty() && (value.front() == 'r' || value.front() == 'R')) {
    is_raw = true;
    value.remove_prefix(1);
  }
  if (!value.empty() && (value.front() == 'b' || value.front() == 'B')) {
    is_bytes = true;
    value.remove_prefix(1);
  }
  if (!is_raw && !value.empty() &&
      (value.front() == 'r' || value.front() == 'R')) {
    is_raw = true;
    value.remove_prefix(1);
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
    return std::string(value);
  }

  if (is_raw) {
    return std::string(value);
  }

  return DecodeStringEscapesImpl(value, is_bytes, is_triple, quote);
}

StatusOr<std::string> DecodeString(const GoogleSqlAstNode& node) {
  size_t component_bytes = 0;
  size_t components = 0;
  for (const auto& child : node.children) {
    if (child->kind == "StringLiteralComponent") {
      component_bytes += child->detail.size();
      ++components;
    }
  }
  std::string result;
  if (components > 1) {
    result.reserve(component_bytes);
  }
  for (const auto& child : node.children) {
    if (child->kind == "StringLiteralComponent") {
      ASSIGN_OR_RETURN(std::string, hv30550_0,
                       (DecodeSingleComponent(child->detail)));
      if (components == 1) {
        return hv30550_0;
      }
      result += hv30550_0;
    }
  }
  if (components != 0) {
    return result;
  }
  return DecodeSingleComponent(node.detail);
}

namespace {
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
        node.detail == "IS NOT" || node.detail == "IS DISTINCT FROM" ||
        node.detail == "IS NOT DISTINCT FROM") {
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

// Boolean-valued AST node kinds beyond IsBooleanAstNode's operator-level
// checks: IN / LIKE / BETWEEN / quantified comparisons evaluate to BOOL but
// arrive as their own node kinds rather than BinaryExpression.
bool IsPredicateAstNode(const GoogleSqlAstNode& node) {
  return IsBooleanAstNode(node) || node.kind == "InExpression" ||
         node.kind == "LikeExpression" || node.kind == "BetweenExpression" ||
         node.kind == "QuantifiedComparisonExpression";
}
std::deque<SqlUdf>& t_lambda_frame_storage() {
  static thread_local std::deque<SqlUdf> storage;
  return storage;
}
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
Expression WrapIfBoolean(Expression expr, const GoogleSqlAstNode& node) {
  if (IsBooleanAstNode(node)) {
    return CaseExpressionExp(
        {{std::move(expr), ConstantValueExp(Value(std::string("true")))}},
        ConstantValueExp(Value(std::string("false"))));
  }
  return expr;
}

// Strict UTF-8 validator: BYTES -> STRING casts must reject the sequences
// GoogleSQL refuses (surrogates, overlong forms, truncated tails) instead of
// passing mojibake through.
// IsValidUtf8Text now lives in common/string_util.hpp (canonical validator
// shared with the expression layer's proto-CAST gate).
// Validate cast coercibility for literal struct sources. Anything not
// statically checkable is left to the existing runtime behavior untouched.
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
  field_types.reserve(node.children[1]->children.size());
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
  elements.reserve(source.children.size());
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
    if (UpperCopy(std::move(type_name)).find("BYTES") != std::string::npos ||
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
      // Functions whose GoogleSQL result type is always BYTES. Their digests
      // are arbitrary bytes, so LENGTH(MD5(x)) must count bytes (16), not
      // UTF-8 lead bytes.
      static constexpr std::array<std::string_view, 7> kBytesResult = {
          "md5",         "sha1",        "sha256",  "sha512",
          "from_base64", "from_base32", "from_hex"};
      if (std::ranges::find(kBytesResult, fn) != kBytesResult.end()) {
        return true;
      }
    }
  }
  return false;
}

// Wraps `key` in the COLLATE(key, 'spec') call a `COLLATE` clause means.
// GoogleSQL requires a plain string literal as the collator; anything else
// (a column, a parameter) is left alone, which is also what the function-call
// form of COLLATE validates at its own call site.
StatusOr<Expression> MakeCollateCall(Expression key,
                                     const GoogleSqlAstNode& collate) {
  const GoogleSqlAstNode* spec = nullptr;
  for (const auto& child : collate.children) {
    if (child->kind == "StringLiteral") {
      spec = child.get();
      break;
    }
  }
  if (spec == nullptr) {
    return key;
  }
  ASSIGN_OR_RETURN(std::string, name, (DecodeString(*spec)));
  return FunctionCallExp(
      "collate", std::vector<Expression>{
                     std::move(key), ConstantValueExp(Value(std::move(name)))});
}

}  // namespace
StatusOr<WindowOrderTerm> ParseOrderingTerm(const GoogleSqlAstNode* term) {
  WindowOrderTerm parsed;
  if (term == nullptr || term->children.empty()) {
    return parsed;
  }
  const GoogleSqlAstNode* collate = nullptr;
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
    // `ORDER BY x COLLATE 'spec'` arrives as a trailing Collate child. The
    // key expression is the collation's *operand*, so the clause can only be
    // applied after the loop: a child after the key expression is the clause,
    // never another operand.
    if (child->kind == "Collate") {
      collate = child.get();
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
  if (collate != nullptr && parsed.expression) {
    ASSIGN_OR_RETURN(Expression, collated,
                     (MakeCollateCall(std::move(parsed.expression), *collate)));
    parsed.expression = std::move(collated);
  }
  return parsed;
}

namespace {
StatusOr<std::vector<WindowOrderTerm>> ParseOrderingList(
    const GoogleSqlAstNode& order) {
  std::vector<WindowOrderTerm> terms;
  terms.reserve(order.children.size());
  for (const GoogleSqlAstNode* term : order.Children("OrderingExpression")) {
    ASSIGN_OR_RETURN(WindowOrderTerm, parsed, (ParseOrderingTerm(term)));
    if (parsed.expression) {
      terms.push_back(std::move(parsed));
    }
  }
  return terms;
}
}  // namespace

namespace {
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

// The NULL guard and the UNNEST source must not duplicate a volatile or
// subquery array argument: sharing the Expression handle only shares the AST
// node, not its per-row evaluation. Bind the argument once as a column of a
// one-row derived table (the same idiom ExpandUdfCall uses for SQL-function
// arguments) and reference that column from both places.
constexpr std::string_view kLambdaArrayColumn = "__lambda_array_arg";

SelectSource MakeLambdaArrayBindingSource(Expression array_expr) {
  auto arg_query = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(
          std::string(kLambdaArrayColumn), std::move(array_expr))},
      std::vector<std::string>{}, Expression{});
  SelectSource source;
  source.query = std::move(arg_query);
  source.alias = "__lambda_array_binding";
  return source;
}

Expression LambdaArrayColumn() {
  return ColumnValueExp(ColumnName("", std::string(kLambdaArrayColumn)));
}

StatusOr<Expression> RewriteLambdaArrayFunction(
    const GoogleSqlAstNode& array_node, const GoogleSqlAstNode& lambda,
    bool as_filter) {
  ASSIGN_OR_RETURN(Expression, hv58424_0, (VisitExpression(array_node)));
  Expression array_expr = std::move(hv58424_0);
  ASSIGN_OR_RETURN(LambdaBindingResult, hv58670_0, (BindLambdaBody(lambda)));
  LambdaBindingResult binding = std::move(hv58670_0);
  auto inner = std::make_shared<SelectStatement>(
      as_filter ? std::vector<NamedExpression>{NamedExpression(
                      std::string(""),
                      ColumnValueExp(ColumnName("", binding.element_binding)))}
                : std::vector<NamedExpression>{NamedExpression(
                      std::string(""), std::move(binding.body))},
      std::vector<std::string>{}, as_filter ? binding.body : Expression{});
  inner->SetSources({MakeLambdaSource(LambdaArrayColumn(), binding)});
  inner->MarkComplex();
  auto query_expression = std::make_shared<QueryExpression>(
      std::move(inner), nullptr, false, false);
  query_expression->SetArrayResult(true);
  // ARRAY_FILTER keeps the input's element type, so it is inferred from the
  // array argument; ARRAY_TRANSFORM's element type comes from the lambda
  // body. Unknown stays empty and the runtime infers it from the produced
  // elements.
  if (as_filter) {
    ASSIGN_OR_RETURN(std::string, h_elem_sql,
                     (InferArrayElementSqlType(array_node)));
    if (h_elem_sql.starts_with("ARRAY<") && h_elem_sql.back() == '>') {
      h_elem_sql = h_elem_sql.substr(6, h_elem_sql.size() - 7);
    } else {
      h_elem_sql.clear();
    }
    query_expression->SetArrayElementSqlType(std::move(h_elem_sql));
  } else {
    ASSIGN_OR_RETURN(
        std::string, h_elem_sql,
        (InferArrayElementSqlType(lambda.children.back().get() == nullptr
                                      ? lambda
                                      : *lambda.children.back())));
    query_expression->SetArrayElementSqlType(std::move(h_elem_sql));
  }
  Expression array_result(query_expression);
  // UNNEST(NULL) is empty, so the NULL guard has to live outside the ARRAY
  // subquery. Both the guard and the UNNEST source read the bound column, so
  // the array argument is evaluated exactly once per row.
  auto outer = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(
          std::string(""),
          CaseExpressionExp({{UnaryExpressionExp(LambdaArrayColumn(),
                                                 UnaryOperation::kIsNull),
                              ConstantValueExp(Value())}},
                            std::move(array_result)))},
      std::vector<std::string>{}, Expression{});
  outer->SetSources({MakeLambdaArrayBindingSource(std::move(array_expr))});
  outer->MarkComplex();
  return QueryExpressionExp(std::move(outer));
}

// ARRAY_INCLUDES(array, e -> pred) asks whether any element satisfies the
// predicate: three-valued over a NULL array (NULL), otherwise TRUE when the
// existential subquery finds a row and FALSE when it does not.  An empty
// array yields FALSE because UNNEST produces no rows to satisfy it.
StatusOr<Expression> RewriteLambdaIncludes(const GoogleSqlAstNode& array_node,
                                           const GoogleSqlAstNode& lambda) {
  ASSIGN_OR_RETURN(Expression, hv60357_0, (VisitExpression(array_node)));
  Expression array_expr = std::move(hv60357_0);
  ASSIGN_OR_RETURN(LambdaBindingResult, hv60581_0, (BindLambdaBody(lambda)));
  LambdaBindingResult binding = std::move(hv60581_0);
  auto inner = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{NamedExpression(
          std::string(""),
          ColumnValueExp(ColumnName("", binding.element_binding)))},
      std::vector<std::string>{}, std::move(binding.body));
  inner->SetSources({MakeLambdaSource(LambdaArrayColumn(), binding)});
  inner->MarkComplex();
  auto exists =
      std::make_shared<QueryExpression>(std::move(inner), nullptr, true, false);
  Expression guarded = CaseExpressionExp(
      {{UnaryExpressionExp(LambdaArrayColumn(), UnaryOperation::kIsNull),
        ConstantValueExp(Value())},
       {Expression(std::move(exists)), ConstantValueExp(Value(int64_t{1}))}},
      ConstantValueExp(Value(int64_t{0})));
  auto outer = std::make_shared<SelectStatement>(
      std::vector<NamedExpression>{
          NamedExpression(std::string(""), std::move(guarded))},
      std::vector<std::string>{}, Expression{});
  outer->SetSources({MakeLambdaArrayBindingSource(std::move(array_expr))});
  outer->MarkComplex();
  return QueryExpressionExp(std::move(outer));
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
  // MEDIAN(x) [OVER (...)] ≡ PERCENTILE_CONT(x, 0.5): one alias here makes
  // both the aggregate and window routes reuse the exact interpolation
  // implementation instead of carrying a second median accumulator.
  const bool median_alias = name == "median";
  if (median_alias) {
    name = "percentile_cont";
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
  // BigQuery exposes the DATE parts as plain one-argument functions
  // (DAY(x), DAYOFWEEK(x), WEEKDAY(x), ...) alongside EXTRACT(part FROM x).
  // Lower them to the extract_* calls the EXTRACT path produces so both
  // spellings share one evaluator implementation. The 2-argument WEEK(x,
  // week_day) form changes the week start and is left to its own error.
  static const std::unordered_map<std::string_view, std::string_view>
      kDatePartFunctionAliases = {
          {"day", "day"},
          {"dayofweek", "dayofweek"},
          {"dayofyear", "dayofyear"},
          {"month", "month"},
          {"quarter", "quarter"},
          {"year", "year"},
          {"isoyear", "isoyear"},
          {"isoweek", "isoweek"},
          {"week", "week"},
          {"weekday", "weekday"},
      };
  {
    const auto alias = kDatePartFunctionAliases.find(name);
    if (alias != kDatePartFunctionAliases.end()) {
      size_t argument_count = 0;
      for (size_t i = 1; i < node.children.size(); ++i) {
        if (node.children[i]->kind != "Location") {
          ++argument_count;
        }
      }
      if (argument_count == 1) {
        // WEEK(<weekday>) is the date_part spelling with an explicit
        // week-start day (LAST_DAY(d, WEEK(MONDAY)), EXTRACT(WEEK(SUNDAY)
        // FROM d)); it must reach its consumer as the "week(<weekday>)"
        // token, not as the one-argument week-number function, whose
        // rewrite resolved the bare weekday as a column.
        if (alias->first == "week") {
          if (auto token = WeekdayDatePartToken(node)) {
            return {ConstantValueExp(Value(std::move(*token)))};
          }
        }
        if (FindUdf(name) == nullptr) {
          name = std::string("extract_") + std::string(alias->second);
        }
      }
    }
  }
  // T2233: GoogleSQL named arguments (name => value) reach the visitor as
  // NamedArgument nodes; the generic argument loop below visits children as
  // plain expressions, where a NamedArgument falls out of the visitor as
  // "unsupported expression".  The TVF route has its own handler in the
  // table-source layer; for scalar calls, support is per-function because
  // the engine has no general parameter-name table.  MAKE_INTERVAL is the
  // GoogleSQL-defined named-argument function: `year/month/day/hour/
  // minute/second => value`, each desugared to the engine's
  // make_interval(value, 'unit') plus interval addition (verified
  // supported: make_interval(1,'day') + make_interval(2,'hour') folds to a
  // compound interval).  Positional spellings keep flowing to the generic
  // loop untouched, so the engine's own make_interval(x, 'unit') surface
  // is unaffected.
  if (name == "make_interval") {
    bool has_named = false;
    for (size_t i = 1; i < node.children.size(); ++i) {
      if (node.children[i]->kind == "NamedArgument") {
        has_named = true;
        break;
      }
    }
    if (has_named) {
      static constexpr std::array<std::string_view, 6> kIntervalSlots = {
          "year", "month", "day", "hour", "minute", "second"};
      std::array<Expression, 6> slots;
      size_t next_positional = 0;
      bool seen_named = false;
      for (size_t i = 1; i < node.children.size(); ++i) {
        const GoogleSqlAstNode& child = *node.children[i];
        if (child.kind == "Location") {
          continue;
        }
        Expression value;
        std::string_view unit;
        if (child.kind == "NamedArgument") {
          seen_named = true;
          const GoogleSqlAstNode* name_node = child.Child("Identifier");
          if (name_node == nullptr) {
            return AstError<Expression>(
                "GoogleSQL AST: MAKE_INTERVAL named argument without a name");
          }
          ASSIGN_OR_RETURN(std::string, arg_name, (Identifier(*name_node)));
          unit = std::string_view(arg_name);
          const auto slot =  // NOLINT(readability-qualified-auto)
              std::find(kIntervalSlots.begin(), kIntervalSlots.end(), unit);
          if (slot == kIntervalSlots.end()) {
            return AstError<Expression>(
                "GoogleSQL AST: MAKE_INTERVAL unknown argument name " +
                arg_name);
          }
          const size_t index =  // NOLINT(modernize-use-auto)
              static_cast<size_t>(slot - kIntervalSlots.begin());
          if (slots[index]) {
            return AstError<Expression>(
                "GoogleSQL AST: MAKE_INTERVAL argument " + arg_name +
                " given twice");
          }
          const GoogleSqlAstNode* value_node = nullptr;
          for (const auto& grandchild : child.children) {
            if (grandchild->kind == "Location" ||
                grandchild->kind == "Identifier") {
              continue;
            }
            value_node = grandchild.get();
            break;
          }
          if (value_node == nullptr) {
            return AstError<Expression>(
                "GoogleSQL AST: MAKE_INTERVAL argument " + arg_name +
                " has no value");
          }
          ASSIGN_OR_RETURN(Expression, hv_named,
                           (VisitExpression(*value_node)));
          slots[index] = std::move(hv_named);
          continue;
        }
        if (seen_named) {
          return AstError<Expression>(
              "GoogleSQL AST: MAKE_INTERVAL positional argument after a "
              "named argument");
        }
        if (next_positional >= kIntervalSlots.size()) {
          return AstError<Expression>(
              "GoogleSQL AST: MAKE_INTERVAL takes at most 6 positional "
              "arguments");
        }
        ASSIGN_OR_RETURN(Expression, hv_pos, (VisitExpression(child)));
        slots[next_positional++] = std::move(hv_pos);
      }
      Expression combined;
      for (size_t index = 0; index < kIntervalSlots.size(); ++index) {
        if (!slots[index]) {
          continue;
        }
        Expression part = FunctionCallExp(
            "make_interval",
            {std::move(slots[index]),
             ConstantValueExp(Value(std::string(kIntervalSlots[index])))});
        combined = combined ? BinaryExpressionExp(std::move(combined),
                                                  BinaryOperation::kAdd,
                                                  std::move(part))
                            : std::move(part);
      }
      if (!combined) {
        return IntervalExpressionExp(0, "second");
      }
      return combined;
    }
  }
  std::vector<Expression> arguments;
  arguments.reserve(node.children.size());
  AggregateHavingModifier having = AggregateHavingModifier::kNone;
  Expression having_condition;
  Expression where_filter;
  std::vector<WindowOrderTerm> inner_order_by;
  std::optional<size_t> inner_limit;
  bool respect_nulls = false;
  bool ignore_nulls = false;
  RecoverNullTreatment(node, &respect_nulls, &ignore_nulls);

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
      // AGG(x HAVING MAX cond): detail may carry MAX or MIN, but the
      // pinned parser leaves it empty -- fall back to the source byte
      // range, where the modifier keyword is the word after "HAVING".
      std::string modifier = UpperCopy(child.detail);
      if (modifier.find("MIN") == std::string::npos &&
          modifier.find("MAX") == std::string::npos) {
        modifier = UpperCopy(SliceSource(&child));
        const size_t kw = modifier.find("HAVING");
        modifier =
            kw == std::string::npos ? std::string() : modifier.substr(kw + 6);
        const size_t first = modifier.find_first_not_of(" \t\n");
        modifier = first == std::string::npos ? std::string()
                                              : modifier.substr(first, 3);
      }
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
          inner_limit = WindowLimitFromUint64(hv65738_0);
        } else {
          // A non-literal LIMIT (e.g. `1+1`, `CAST(2 AS INT64)`) must not be
          // silently dropped: that would return the whole array instead of
          // the requested prefix. Try a constant fold; otherwise reject.
          ASSIGN_OR_RETURN(Expression, limit_expr,
                           (VisitExpression(*limit_node)));
          if (limit_expr && limit_expr->Type() == TypeTag::kConstantValue) {
            const Value& cv = limit_expr->AsConstantValue().GetValue();
            if (cv.type == ValueType::kInt64 && !cv.IsNull() &&
                cv.value.int_value >= 0) {
              // kInt64 stores the value in the int_value bit pattern (already
              // range-checked >= 0 above); IsUnsigned() adds no information.
              inner_limit = WindowLimitFromUint64(
                  static_cast<uint64_t>(cv.value.int_value));
            } else {
              return AstError<Expression>(
                  "aggregate LIMIT must be a non-negative integer constant");
            }
          } else {
            return AstError<Expression>(
                "aggregate LIMIT must be a constant integer");
          }
        }
      }
      continue;
    }
    ASSIGN_OR_RETURN(Expression, hv65853_0, (VisitExpression(child)));
    Expression arg = std::move(hv65853_0);
    if (name == "concat" || name == "safe.concat") {
      arg = WrapIfBoolean(std::move(arg), child);
    }
    arguments.push_back(std::move(arg));
  }
  if (median_alias) {
    arguments.push_back(ConstantValueExp(Value(0.5)));
  }

  auto finish_aggregate = [&](AggregationType type) -> StatusOr<Expression> {
    // GoogleSQL only orders/limits the aggregates whose result keeps input
    // order (ARRAY_AGG, STRING_AGG, ARRAY_CONCAT_AGG).  COUNT(DISTINCT v
    // ORDER BY u LIMIT 1) and friends are rejected rather than silently
    // ignoring (or half-applying) the modifier.
    if ((!inner_order_by.empty() || inner_limit.has_value()) &&
        type != AggregationType::kArrayAgg &&
        type != AggregationType::kStringAgg &&
        type != AggregationType::kArrayConcatAgg) {
      return AstError<Expression>(
          "GoogleSQL AST: ORDER BY / LIMIT inside an aggregate function is "
          "only supported for ARRAY_AGG, STRING_AGG and ARRAY_CONCAT_AGG");
    }
    const bool is_distinct =
        node.detail.find("distinct=true") != std::string::npos;
    // GoogleSQL: with DISTINCT, the inner ORDER BY may only name the
    // aggregated argument (`ARRAY_AGG(DISTINCT a ORDER BY u)` is an error:
    // which `u` would order a collapsed duplicate is undefined).
    if (is_distinct && !inner_order_by.empty()) {
      for (const WindowOrderTerm& term : inner_order_by) {
        const std::string term_text = term.expression->ToString();
        const bool names_argument =
            std::any_of(arguments.begin(), arguments.end(),
                        [&](const Expression& argument) {
                          return argument->ToString() == term_text;
                        });
        if (!names_argument) {
          return AstError<Expression>(
              "GoogleSQL AST: an aggregate function that has both DISTINCT "
              "and ORDER BY arguments can only ORDER BY expressions that are "
              "arguments to the function");
        }
      }
    }
    auto aggregate = std::make_shared<AggregateExpression>(
        type, arguments.empty() ? nullptr : arguments[0], is_distinct);
    if (having != AggregateHavingModifier::kNone) {
      aggregate->SetHaving(having, having_condition);
    }
    if (ignore_nulls) {
      aggregate->SetIgnoreNulls(true);
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
          aggregate->SetArrayElementSqlType(std::move(element_type));
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
      name == "elementwise_avg" || name == "any_value" || name == "mode" ||
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
    const bool arity_ok = !arity_checked || arguments.size() == 1 ||
                          (is_bit && arguments.size() == 2) ||
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
    if (name == "mode") {
      type = AggregationType::kMode;
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
    } else if (name == "regr_slope") {
      extended = AggregationType::kRegrSlope;
      min_arity = max_arity = 2;
    } else if (name == "regr_intercept") {
      extended = AggregationType::kRegrIntercept;
      min_arity = max_arity = 2;
    } else if (name == "regr_count") {
      extended = AggregationType::kRegrCount;
      min_arity = max_arity = 2;
    } else if (name == "regr_r2") {
      extended = AggregationType::kRegrR2;
      min_arity = max_arity = 2;
    } else if (name == "regr_avgx") {
      extended = AggregationType::kRegrAvgX;
      min_arity = max_arity = 2;
    } else if (name == "regr_avgy") {
      extended = AggregationType::kRegrAvgY;
      min_arity = max_arity = 2;
    } else if (name == "regr_sxx") {
      extended = AggregationType::kRegrSxx;
      min_arity = max_arity = 2;
    } else if (name == "regr_syy") {
      extended = AggregationType::kRegrSyy;
      min_arity = max_arity = 2;
    } else if (name == "regr_sxy") {
      extended = AggregationType::kRegrSxy;
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
    } else if (name == "percentile_disc") {
      // T2243: PERCENTILE_DISC is an aggregate AND an analytic function; the
      // window route already existed, the aggregate alias did not.
      extended = AggregationType::kPercentileDisc;
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
  if (FindUdf(name) != nullptr) {
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
  // T2232: SAFE.<fn>(...) is GoogleSQL's blanket error-suppression prefix.
  // NULLIFERROR(expr) already carries exactly that contract (any error from
  // the operand evaluates to NULL, lazily, in both evaluators), so desugar
  // SAFE.FOO(a, b) into NULLIFERROR(FOO(a, b)) at the single call-building
  // exit. SAFE.CAST/EXTRACT/IF are operator syntax in GoogleSQL (safe_cast
  // is handled with the CAST node) and never reach this generic path with a
  // usable shape.
  if (name.starts_with("safe.") && name.size() > 5) {
    const std::string base = name.substr(5);
    if (base == "cast" || base == "extract" || base == "if") {
      return AstError<Expression>(
          "GoogleSQL AST: SAFE. prefix does not apply to " + name);
    }
    return FunctionCallExp("nulliferror",
                           {FunctionCallExp(base, std::move(arguments))});
  }
  return FunctionCallExp(name, std::move(arguments));
}
StatusOr<Expression> ExpandUdfCall(const std::string& name,
                                   std::vector<Expression> arguments) {
  SqlUdf* found = FindUdf(name);
  if (found == nullptr) {
    return Status(Status::kNotExists, "udf not registered");
  }
  SqlUdf& udf = *found;
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
bool IsArrayTypeNode(std::string_view kind) { return kind == "ArrayType"; }

}  // namespace
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
        params.reserve(child->children.size());
        for (const auto& param : child->children) {
          if (param->kind == "IntLiteral" || param->kind == "Identifier" ||
              param->kind == "StringLiteral") {
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
    fields.reserve(node.children.size());
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

namespace {
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

  // Predicate-shaped nodes produce BOOL, which the runtime cannot recover
  // from stored values (BOOL folds into kInt64): they need the static tag.
  if (IsPredicateAstNode(node)) {
    return "BOOL";
  }
  if (node.kind == "ConcatExpr") {
    // `a || b` is array concatenation when every operand is an array;
    // otherwise the result is a string.
    std::string array_type;
    for (const auto& child : node.children) {
      if (IsAstTrivia(child->kind)) {
        continue;
      }
      ASSIGN_OR_RETURN(std::string, h_tmp_cat,
                       (InferArrayElementSqlType(*child)));
      if (h_tmp_cat.empty()) {
        return std::string{};  // array-vs-string concat is undecidable
      }
      if (!h_tmp_cat.starts_with("ARRAY<")) {
        return "STRING";
      }
      if (array_type.empty()) {
        array_type = std::move(h_tmp_cat);
      }
    }
    return array_type.empty() ? std::string("STRING") : array_type;
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
  // Anything else (identifiers, function calls, arithmetic) reports unknown
  // so the runtime infers the element type from produced values — a wrong
  // "INT64" guess would tag e.g. string-producing lambda results as
  // ARRAY<INT64>.
  return std::string{};
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
      return "FLOAT64";  // match InferArrayElementSqlType's canonical spelling
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
    // Predicate projections produce BOOL, which runtime values cannot
    // distinguish from INT64.
    if (IsPredicateAstNode(expr)) {
      return "BOOL";
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
  // Predicate-shaped arguments produce BOOL, which runtime values cannot
  // distinguish from INT64 (both store as kInt64).
  if (IsPredicateAstNode(node)) {
    return "BOOL";
  }
  return std::string{};
}

StatusOr<std::string> DecodeBytes(const GoogleSqlAstNode& node) {
  std::string result;
  result.reserve(node.children.size());
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

}  // namespace
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
      names->insert(Lower(std::move(alias)));
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
            names->insert(Lower(std::move(unnest_alias)));
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
              names->insert(Lower(std::move(name)));
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

namespace {
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

void RecoverNullTreatment(const GoogleSqlAstNode& call, bool* respect_nulls,
                          bool* ignore_nulls) {
  if (t_visit_source == nullptr || call.start > call.end ||
      call.end > t_visit_source->size()) {
    return;
  }
  std::string text = SliceSource(&call);
  for (const auto& child : call.children) {
    if (child->start < call.start || child->end < child->start ||
        child->end > call.end) {
      continue;
    }
    std::fill(
        text.begin() + static_cast<std::ptrdiff_t>(child->start - call.start),
        text.begin() + static_cast<std::ptrdiff_t>(child->end - call.start),
        ' ');
  }

  size_t position = 0;
  std::string pending;
  while (position < text.size()) {
    SkipSqlTrivia(text, &position);
    if (position >= text.size()) {
      break;
    }
    if (!IsLateralIdentifierChar(text[position])) {
      pending.clear();
      ++position;
      continue;
    }
    const size_t begin = position;
    while (position < text.size() && IsLateralIdentifierChar(text[position])) {
      ++position;
    }
    std::string word(text.substr(begin, position - begin));
    for (char& character : word) {
      character = static_cast<char>(
          std::toupper(static_cast<unsigned char>(character)));
    }
    if (word == "IGNORE" || word == "RESPECT") {
      pending = std::move(word);
      continue;
    }
    if (word == "NULLS" && pending == "IGNORE") {
      *ignore_nulls = true;
      pending.clear();
    } else if (word == "NULLS" && pending == "RESPECT") {
      *respect_nulls = true;
      pending.clear();
    } else {
      pending.clear();
    }
  }
}

}  // namespace

StatusOr<NamedWindowParts> ParseWindowSpecification(
    const GoogleSqlAstNode& spec) {
  NamedWindowParts parts;
  if (const auto* inherited = spec.Child("Identifier")) {
    ASSIGN_OR_RETURN(std::string, name, Identifier(*inherited));
    const auto found = t_named_windows.find(Lower(name));
    if (found == t_named_windows.end()) {
      return AstError<NamedWindowParts>("GoogleSQL AST: unknown window " +
                                        name);
    }
    parts = found->second;
    // OVER (w ORDER BY ... ROWS ...) may only ADD clauses the base window
    // lacks: GoogleSQL rejects PARTITION BY here, an ORDER BY when `w` has
    // one, and any refinement of a window that already has a frame.
    // Silently merging / replacing them changed the result.
    if (spec.Child("PartitionBy") != nullptr) {
      return AstError<NamedWindowParts>(
          "GoogleSQL AST: PARTITION BY cannot refine named window " + name);
    }
    if (spec.Child("OrderBy") != nullptr && !parts.order_by.empty()) {
      return AstError<NamedWindowParts>(
          "GoogleSQL AST: ORDER BY cannot override named window " + name);
    }
    if (parts.has_frame && (spec.Child("OrderBy") != nullptr ||
                            spec.Child("WindowFrame") != nullptr)) {
      return AstError<NamedWindowParts>("GoogleSQL AST: named window " + name +
                                        " with a frame cannot be "
                                        "refined");
    }
  }
  if (const GoogleSqlAstNode* partition = spec.Child("PartitionBy")) {
    for (const auto& child : partition->children) {
      if (child->kind == "Location") {
        continue;
      }
      // Partition keys are arbitrary expressions (`PARTITION BY grouping(a)
      // + grouping(b)`, `UPPER(x)`), not just column paths; dropping
      // non-path children silently changed which rows share a partition.
      ASSIGN_OR_RETURN(Expression, hv92026_0, (VisitExpression(*child)));
      parts.partition_by.push_back(std::move(hv92026_0));
    }
  }
  if (const GoogleSqlAstNode* order = spec.Child("OrderBy")) {
    ASSIGN_OR_RETURN(std::vector<WindowOrderTerm>, h_tmp_2553,
                     (ParseOrderingList(*order)));
    parts.order_by = std::move(h_tmp_2553);
  }
  if (const GoogleSqlAstNode* frame = spec.Child("WindowFrame")) {
    // T3697: GoogleSQL has three frame units; the old binary test turned
    // `GROUPS BETWEEN ...` into ROWS, evaluating peer-group frames as plain
    // row offsets (window_eval's kGroups arm was unreachable from SQL).
    const std::string frame_unit_text = UpperCopy(frame->detail);
    parts.frame_unit = frame_unit_text == "RANGE"    ? WindowFrameUnit::kRange
                       : frame_unit_text == "GROUPS" ? WindowFrameUnit::kGroups
                                                     : WindowFrameUnit::kRows;
    const auto bounds = frame->Children("WindowFrameExpr");
    // Single-bound frames (`ROWS UNBOUNDED PRECEDING`, `ROWS 3 PRECEDING`,
    // `ROWS CURRENT ROW`) mean BETWEEN <bound> AND CURRENT ROW; a FOLLOWING
    // start has no single-bound form. (Corpus row_number_3 rejects ROWS on
    // ROW_NUMBER because ranking functions take no frame at all; see
    // VisitAnalyticFunctionCall.)
    if (bounds.size() == 1) {
      parts.has_frame = true;
      ASSIGN_OR_RETURN(WindowFrameBound, single_bound,
                       (ParseFrameBound(*bounds[0])));
      if (single_bound.type == WindowFrameBoundType::kOffsetFollowing ||
          single_bound.type == WindowFrameBoundType::kUnboundedFollowing) {
        return AstError<NamedWindowParts>(
            "GoogleSQL AST: a single-bound window frame cannot start with "
            "FOLLOWING");
      }
      parts.frame_start = std::move(single_bound);
      parts.frame_end = WindowFrameBound{
          .type = WindowFrameBoundType::kCurrentRow, .offset = nullptr};
    } else if (bounds.size() >= 2) {
      parts.has_frame = true;
      ASSIGN_OR_RETURN(WindowFrameBound, h_tmp_2564,
                       (ParseFrameBound(*bounds[0])));
      parts.frame_start = std::move(h_tmp_2564);
      ASSIGN_OR_RETURN(WindowFrameBound, h_tmp_2565,
                       (ParseFrameBound(*bounds[1])));
      parts.frame_end = std::move(h_tmp_2565);
    } else {
      return AstError<NamedWindowParts>(
          "GoogleSQL AST: window frame has no bounds");
    }
  }
  return parts;
}

namespace {
StatusOr<Expression> VisitAnalyticFunctionCall(const GoogleSqlAstNode& node) {
  const GoogleSqlAstNode* call = node.Child("FunctionCall");
  if (call == nullptr || call->children.empty()) {
    return AstError<Expression>(
        "GoogleSQL AST: malformed analytic function call");
  }
  auto window = std::make_shared<WindowFunctionCallExpression>();
  // Set when the call is the MEDIAN(x) OVER (...) alias; the synthesized
  // PERCENTILE_CONT percentile argument is appended after the real ones.
  bool median_window_alias = false;

  size_t arg_start = 0;
  if (call->children[0]->kind == "PathExpression") {
    ASSIGN_OR_RETURN(std::string, hv93668_0, (Path(*call->children[0])));
    std::string fn = UpperCopy(std::move(hv93668_0));
    if (fn == "MEDIAN") {
      // Same PERCENTILE_CONT(x, 0.5) alias as the aggregate route; the
      // window evaluator dispatches on this exact name.
      window->function = "PERCENTILE_CONT";
      median_window_alias = true;
    } else {
      window->function = std::move(fn);
    }
    arg_start = 1;
  } else {
    return AstError<Expression>("GoogleSQL AST: anonymous analytic function");
  }
  if (call->detail.find("distinct=true") != std::string::npos) {
    window->distinct = true;
  }
  RecoverNullTreatment(*call, &window->respect_nulls, &window->ignore_nulls);

  std::vector<WindowOrderTerm> inner_order_by;
  inner_order_by.reserve(call->children.size());
  for (size_t i = arg_start; i < call->children.size(); ++i) {
    const GoogleSqlAstNode& child = *call->children[i];
    if (child.kind == "Location") {
      continue;
    }
    if (child.kind == "WhereClause") {
      // AGG(x WHERE cond) OVER (...): row-level pre-filter.
      if (child.children.empty()) {
        return AstError<Expression>(
            "GoogleSQL AST: empty WhereClause in aggregate call");
      }
      ASSIGN_OR_RETURN(Expression, hv94301_0,
                       (VisitExpression(*child.children[0])));
      window->where_filter = std::move(hv94301_0);
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
          window->inner_limit = WindowLimitFromUint64(hv94767_0);
        } else {
          ASSIGN_OR_RETURN(Expression, limit_expr,
                           (VisitExpression(*limit_node)));
          if (limit_expr && limit_expr->Type() == TypeTag::kConstantValue) {
            const Value& cv = limit_expr->AsConstantValue().GetValue();
            if (cv.type == ValueType::kInt64 && !cv.IsNull() &&
                cv.value.int_value >= 0) {
              // kInt64 stores the value in the int_value bit pattern (already
              // range-checked >= 0 above); IsUnsigned() adds no information.
              window->inner_limit = WindowLimitFromUint64(
                  static_cast<uint64_t>(cv.value.int_value));
            } else {
              return AstError<Expression>(
                  "aggregate LIMIT must be a non-negative integer constant");
            }
          } else {
            return AstError<Expression>(
                "aggregate LIMIT must be a constant integer");
          }
        }
      }
      continue;
    }
    ASSIGN_OR_RETURN(Expression, hv94904_0, (VisitExpression(child)));
    window->args.push_back(std::move(hv94904_0));
  }
  if (window->function == "COUNT" && window->args.size() != 1) {
    return AstError<Expression>("GoogleSQL AST: COUNT requires one argument");
  }
  if (median_window_alias) {
    window->args.push_back(ConstantValueExp(Value(0.5)));
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
      const auto found = t_named_windows.find(Lower(name));
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
  // Numbering and navigation functions take no window frame.
  if (window->has_frame) {
    const std::string fn = UpperCopy(window->function);
    if (fn == "ROW_NUMBER" || fn == "RANK" || fn == "DENSE_RANK" ||
        fn == "PERCENT_RANK" || fn == "CUME_DIST" || fn == "NTILE" ||
        fn == "LAG" || fn == "LEAD") {
      return AstError<Expression>(
          "Window framing clause is not allowed for analytic function " +
          Lower(fn));
    }
  }
  return window;
}

// Normalizes a TIMESTAMP string to UTC ("...+00"), interpreting an explicit
// offset / UTC marker when present and the session default time zone
// otherwise.  Shared by TIMESTAMP literals and typed array elements.
std::optional<std::string> NormalizeTimestampTextImpl(const std::string& text) {
  if (text.size() < 10) {
    return std::nullopt;  // too short for YYYY-MM-DD
  }
  size_t tz_pos = std::string::npos;
  for (size_t i = 10; i < text.size(); ++i) {
    if (text[i] == '+' || text[i] == '-') {
      tz_pos = i;
      break;
    }
  }
  // Keep the offset in seconds: named/default zones (and tzdata LMT periods)
  // frequently carry a sub-minute offset, and truncating to minutes shifts
  // such timestamps by up to 59 seconds.
  int total_offset_secs = 0;
  bool has_explicit_tz = false;
  if (text.find("UTC") != std::string::npos ||
      text.find("utc") != std::string::npos ||
      text.find('Z') != std::string::npos ||
      text.find('z') != std::string::npos) {
    total_offset_secs = 0;
    has_explicit_tz = true;
  }
  std::string base_time = text;
  if (tz_pos != std::string::npos) {
    base_time = text.substr(0, tz_pos);
    const char sign = text[tz_pos];
    const std::string tz_part = text.substr(tz_pos + 1);
    // GoogleSQL requires [+-]HH[:MM] or [+-]HHMM with bounded fields. The old
    // parser treated anything not four digits as a bare hour ("+530" became
    // +530 hours) and swallowed parse errors as UTC, so a malformed literal
    // silently produced a wildly wrong instant. Reject it instead.
    const auto all_digits = [](std::string_view s) {
      if (s.empty()) {
        return false;
      }
      for (char c : s) {  // NOLINT(readability-use-anyofallof)
        if (std::isdigit(static_cast<unsigned char>(c)) == 0) {
          return false;
        }
      }
      return true;
    };
    int tz_hours = 0;
    int tz_mins = 0;
    bool valid_offset = false;
    const size_t colon = tz_part.find(':');
    if (colon != std::string::npos) {
      const std::string hours = tz_part.substr(0, colon);
      const std::string mins = tz_part.substr(colon + 1);
      if (hours.size() <= 2 && mins.size() == 2 && all_digits(hours) &&
          all_digits(mins)) {
        tz_hours = std::stoi(hours);
        tz_mins = std::stoi(mins);
        valid_offset = tz_hours <= 14 && tz_mins <= 59;
      }
    } else if (all_digits(tz_part)) {
      if (tz_part.size() == 4) {
        tz_hours = std::stoi(tz_part.substr(0, 2));
        tz_mins = std::stoi(tz_part.substr(2, 2));
        valid_offset = tz_hours <= 14 && tz_mins <= 59;
      } else if (tz_part.size() <= 2) {
        tz_hours = std::stoi(tz_part);
        valid_offset = tz_hours <= 14;
      }
    }
    if (!valid_offset) {
      return std::nullopt;
    }
    total_offset_secs =
        (tz_hours * 3600 + tz_mins * 60) * (sign == '-' ? -1 : 1);
    has_explicit_tz = true;
  }
  if (base_time.size() > 10 && (base_time[10] == 'T' || base_time[10] == 't')) {
    base_time[10] = ' ';
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
    // No date fields at all: the text is garbage, not a TIMESTAMP literal.
    // Letting it through would hand a plain string constant to callers and
    // silently turn TIMESTAMP comparisons into string comparisons.
    return std::nullopt;
  }
  // Validate the calendar date and clock fields: TimeGm below normalizes
  // out-of-range fields like mktime, so 2026-02-29 used to roll silently to
  // March 1 and second=60 (leap second) used to advance a minute. BigQuery
  // rejects both.
  if (Y < 1 || Y > 9999 || M < 1 || M > 12 || h < 0 || h > 23 || m < 0 ||
      m > 59 || s_val < 0 || s_val >= 60) {
    return std::nullopt;
  }
  {
    const std::chrono::year_month_day ymd{
        std::chrono::year{Y}, std::chrono::month{static_cast<unsigned>(M)},
        std::chrono::day{static_cast<unsigned>(D)}};
    if (!ymd.ok()) {
      return std::nullopt;
    }
  }
  // TIMESTAMP literals accept only a numeric offset or the UTC/GMT/Z markers
  // as the zone (BigQuery rejects named zones here). A trailing alphabetic
  // name such as "JST" used to fall through sscanf and silently become UTC.
  std::string named_zone;
  size_t zone_alpha = std::string::npos;
  for (size_t i = 11; i < base_time.size(); ++i) {
    if (std::isalpha(static_cast<unsigned char>(base_time[i])) != 0) {
      zone_alpha = i;
      break;
    }
  }
  if (zone_alpha != std::string::npos) {
    std::string tail_raw = base_time.substr(zone_alpha);
    const size_t lead = tail_raw.find_first_not_of(" \t");
    tail_raw =
        lead == std::string::npos ? std::string() : tail_raw.substr(lead);
    std::string tail = tail_raw;
    for (char& ch : tail) {
      ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    // A named zone is accepted only when it names a real zone in the tz
    // database (Europe/London, America/New_York, ...); the UTC/GMT markers
    // and their numeric forms stay allowed. An unknown abbreviation such as
    // "JST" is rejected instead of silently becoming UTC.
    const bool allowed = tail == "Z" || tail == "UT" || tail == "UTC" ||
                         tail == "GMT" || tail.starts_with("UTC+") ||
                         tail.starts_with("UTC-") || tail.starts_with("GMT+") ||
                         tail.starts_with("GMT-") || tail == "ETC/GREENWICH" ||
                         tail == "ETC/UTC" || tail == "ETC/GMT" ||
                         platform::IsKnownTimeZone(tail_raw);
    if (!allowed) {
      return std::nullopt;
    }
    // An accepted IANA zone name must contribute its own offset; otherwise
    // the literal would be silently interpreted in the session zone while
    // carrying the zone's name (and a name with Z/z could be mis-detected as
    // a UTC marker above).
    if (tail != "Z" && tail != "UT" && tail != "UTC" && tail != "GMT" &&
        !tail.starts_with("UTC+") && !tail.starts_with("UTC-") &&
        !tail.starts_with("GMT+") && !tail.starts_with("GMT-") &&
        platform::IsKnownTimeZone(tail_raw)) {
      named_zone = tail_raw;
      has_explicit_tz = true;
    }
  }
  if (!has_explicit_tz) {
    total_offset_secs = ParseTimeZoneOffset(GetDefaultTimeZone(), Y, M, D, h, m,
                                            static_cast<int>(s_val));
  } else if (!named_zone.empty()) {
    total_offset_secs =
        ParseTimeZoneOffset(named_zone, Y, M, D, h, m, static_cast<int>(s_val));
  }
  struct tm t = {};
  t.tm_year = Y - 1900;
  t.tm_mon = M - 1;
  t.tm_mday = D;
  t.tm_hour = h;
  t.tm_min = m;
  // TimeGm/timegm normalize out-of-range fields, so subtracting the whole
  // offset (including sub-minute seconds) here is safe.
  t.tm_sec = static_cast<int>(s_val) - total_offset_secs;
  t.tm_isdst = 0;
  const int64_t epoch = platform::TimeGm(&t);
  struct tm utc = {};
  platform::GmTime(epoch, &utc);
  std::array<char, 64> buf{};
  size_t dot_pos = text.find('.');
  if (dot_pos != std::string::npos) {
    size_t end_digit = dot_pos + 1;
    while (end_digit < text.size() && text[end_digit] >= '0' &&
           text[end_digit] <= '9') {
      ++end_digit;
    }
    if (end_digit == dot_pos + 1) {
      // A trailing '.' with no fractional digits would normalize to the
      // malformed "...:00.+00". GoogleSQL rejects that spelling.
      return std::nullopt;
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

// Validates the clock fields (H:M:S[.fraction]) of a DATETIME/TIME literal.
// `allow_leap_second` admits the 23:59:60 spelling that the DATETIME folder
// carries into the next minute; every other second value must be < 60.
bool IsValidCivilClock(std::string_view s, bool allow_leap_second) {
  size_t pos = 0;
  const auto number = [&s, &pos](int* out) {
    if (pos >= s.size() || s[pos] < '0' || '9' < s[pos]) {
      return false;
    }
    int value = 0;
    size_t digits = 0;
    while (pos < s.size() && '0' <= s[pos] && s[pos] <= '9') {
      if (++digits > 9) {
        return false;  // cap the field length so `value` cannot overflow
      }
      value = (value * 10) + (s[pos++] - '0');
    }
    *out = value;
    return true;
  };
  const auto take = [&s, &pos](char ch) {
    if (pos >= s.size() || s[pos] != ch) {
      return false;
    }
    ++pos;
    return true;
  };
  int hour = 0;
  int minute = 0;
  int second = 0;
  if (!number(&hour) || !take(':') || !number(&minute) || !take(':') ||
      !number(&second)) {
    return false;
  }
  if (hour > 23 || minute > 59 || second > 59) {
    if (!allow_leap_second || hour != 23 || minute != 59 || second != 60) {
      return false;
    }
  }
  if (pos < s.size()) {
    if (!take('.')) {
      return false;  // trailing junk after the clock
    }
    size_t digits = 0;
    while (pos < s.size() && '0' <= s[pos] && s[pos] <= '9') {
      ++pos;
      ++digits;
    }
    if (digits == 0 || pos != s.size()) {
      return false;
    }
  }
  return true;
}

// DATETIME accepts a civil date, a date + clock (T or space separator), and
// the legacy time-only spelling; TIME accepts only the clock. Range-check the
// fields here instead of folding out-of-range text into a string constant.
bool IsValidCivilTimeLiteral(const std::string& text, bool is_datetime) {
  if (!is_datetime) {
    return IsValidCivilClock(text, /*allow_leap_second=*/false);
  }
  if (TryParseDateDays(text).HasValue()) {
    return true;  // date-only DATETIME
  }
  const std::string_view view(text);
  const size_t sep = view.find_first_of(" Tt");
  if (sep == std::string_view::npos) {
    // The pinned parser also accepts DATETIME 'HH:MM:SS'; keep it working,
    // but validate its fields like any other literal.
    return IsValidCivilClock(view, /*allow_leap_second=*/true);
  }
  return TryParseDateDays(view.substr(0, sep)).HasValue() &&
         IsValidCivilClock(view.substr(sep + 1),
                           /*allow_leap_second=*/true);
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
  current.reserve(body.size());
  // Position in `current` of the key/value separator colon: the first ':'
  // seen outside a string at depth 0.  A naive find(':') would cut inside a
  // quoted key such as "a:b".
  size_t sep_pos = std::string::npos;
  auto flush = [&]() {
    if (sep_pos == std::string::npos) {
      return;
    }
    std::string key = current.substr(0, sep_pos);
    while (!key.empty() &&
           std::isspace(static_cast<unsigned char>(key.back()))) {
      key.pop_back();
    }
    if (key.size() >= 2 && key.front() == '"' && key.back() == '"') {
      key = key.substr(1, key.size() - 2);
    }
    fields->emplace_back(std::move(key), current.substr(sep_pos + 1));
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
    if (c == ':' && depth == 0 && sep_pos == std::string::npos) {
      sep_pos = current.size();
    }
    if (c == ',' && depth == 0) {
      flush();
      current.clear();
      sep_pos = std::string::npos;
      continue;
    }
    current.push_back(c);
  }
  if (!current.empty()) {
    flush();
  }
  return true;
}

}  // namespace
// Infer a struct expansion from its actual constructor/projection. Unknown
// shapes must remain an explicit diagnostic instead of inventing field names.
std::optional<std::vector<std::string>> StructProjectionFields(
    const Expression& expression, size_t depth) {
  if (!expression || depth >= 32) {
    return std::nullopt;
  }
  std::vector<std::string> names;
  if (expression->Type() == TypeTag::kConstantValue) {
    const Value& value = expression->AsConstantValue().GetValue();
    if (value.type != ValueType::kVarChar) {
      return std::nullopt;
    }
    const auto object = json_doc::ParseDocument(value.AsStringView());
    if (!object || object->type != json_doc::JVal::kObj) {
      return std::nullopt;
    }
    names.reserve(object->obj.size());
    for (const auto& field : object->obj) {
      names.push_back(field.first);
    }
    return names;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp) {
    const auto& call = expression->AsFunctionCallExpression();
    if (call.FuncName() != "__struct_json__") {
      return std::nullopt;
    }
    const auto& arguments = call.Args();
    names.reserve(arguments.size() / 3);
    for (size_t i = 0; i + 2 < arguments.size(); i += 3) {
      if (arguments[i]->Type() != TypeTag::kConstantValue) {
        return std::nullopt;
      }
      const Value& name = arguments[i]->AsConstantValue().GetValue();
      if (name.type != ValueType::kVarChar) {
        return std::nullopt;
      }
      names.emplace_back(name.AsStringView());
    }
    return names;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const auto& query = expression->AsQueryExpression().Query();
    if (!query) {
      return std::nullopt;
    }
    if (query->AsStruct()) {
      names.reserve(query->SelectList().size());
      for (const NamedExpression& field : query->SelectList()) {
        if (!field.name.empty()) {
          names.push_back(field.name);
        } else if (field.expression &&
                   field.expression->Type() == TypeTag::kColumnValue &&
                   field.expression->AsColumnValue().GetColumnName().name !=
                       "*") {
          names.push_back(
              field.expression->AsColumnValue().GetColumnName().name);
        } else {
          return std::nullopt;
        }
      }
      return names;
    }
    if (query->SelectList().size() == 1) {
      return StructProjectionFields(query->SelectList()[0].expression,
                                    depth + 1);
    }
  }
  return std::nullopt;
}

namespace {
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
  head_fields.reserve(declared == nullptr ? 0 : declared->size());
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

}  // namespace
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
    // Qualified constant names (CREATE CONSTANT pkg.k AS 5) resolve through
    // the same typed expression registry as unqualified ones; only the
    // legacy string registry below stringified them ('5', or '' for a
    // per-reference subquery constant).
    {
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
    return std::make_shared<ConstantValue>(Value(std::move(h_tmp_2964)),
                                           TextDomain::kString);
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
    if (!std::isfinite(parsed)) {
      return AstError<Expression>(
          "GoogleSQL AST: numeric literal out of range " + text);
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
      // Value::Date is a throwing EXC-SHIM wrapper; keep a malformed literal
      // inside the StatusOr boundary instead of letting the exception escape.
      try {
        return ConstantValueExp(Value::Date(text));
      } catch (const std::exception& e) {
        return AstError<Expression>(std::string("invalid DATE literal: ") +
                                    e.what());
      }
    }
    if (node.detail == "TYPE_DATETIME") {
      if (!IsValidCivilTimeLiteral(text, /*is_datetime=*/true)) {
        return AstError<Expression>("GoogleSQL AST: invalid DATETIME literal " +
                                    text);
      }
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
      if (!IsValidCivilTimeLiteral(text, /*is_datetime=*/false)) {
        return AstError<Expression>("GoogleSQL AST: invalid TIME literal " +
                                    text);
      }
      return ConstantValueExp(Value(std::string(text)));
    }
    // TYPE_TIMESTAMP
    std::optional<std::string> normalized_timestamp =
        NormalizeTimestampTextImpl(text);
    if (!normalized_timestamp.has_value()) {
      return AstError<Expression>("GoogleSQL AST: invalid TIMESTAMP literal " +
                                  text);
    }
    return std::make_shared<ConstantValue>(
        Value(std::move(*normalized_timestamp)), TextDomain::kTimestamp);
  }

  if (node.kind == "NullLiteral") {
    return ConstantValueExp(Value());
  }
  // INSERT ... VALUES (30, DEFAULT): the keyword rides a marker function
  // call.  The visitor cannot see the target table, so the INSERT executor
  // substitutes the target column's declared DEFAULT (and rejects the
  // marker outside INSERT VALUES, where it has no meaning).
  if (node.kind == "DefaultLiteral") {
    return FunctionCallExp("__column_default", {});
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
    elements.reserve(node.children.size());
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
        std::optional<std::string> normalized_element =
            NormalizeTimestampTextImpl(hv110933_0);
        if (!normalized_element.has_value()) {
          return AstError<Expression>(
              "GoogleSQL AST: invalid TIMESTAMP array element");
        }
        elements.push_back(
            ConstantValueExp(Value(std::move(*normalized_element))));
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
      // No declared element type: leave empty so TryBuildArray can infer it
      // from the elements.  Injecting "INT64" here would make a declared
      // ARRAY<INT64> indistinguishable from an undeclared one, allowing the
      // inference pass to overwrite an explicit INT64 declaration (P367).
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
    // The callee applies declared names starting at element 0 (unlike the
    // head-based inference path, which starts at element 1), so a single
    // declared STRUCT field is enough; gating this on >= 2 left .a/.f1 wrong
    // for ARRAY<STRUCT<a INT64>>[STRUCT(1)].
    AlignAnonymousStructFieldNames(
        &elements, struct_field_names.empty() ? nullptr : &struct_field_names);
    return ArrayExpressionExp(std::move(elements), std::move(element_type));
  }
  // `a << b` / `a >> b`: the dump interleaves a Location node between the
  // operands.
  if (node.kind == "BitwiseShiftExpression") {
    std::vector<const GoogleSqlAstNode*> operands;
    operands.reserve(node.children.size());
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
    // Unary plus is the identity on its numeric operand; it must not fall
    // into the unary-minus default below.
    if (node.detail == "+") {
      return not_e;
    }
    UnaryOperation op = UnaryOperation::kMinus;
    if (node.detail == "NOT") {
      op = UnaryOperation::kNot;
    } else if (node.detail == "~") {
      op = UnaryOperation::kBitwiseNot;
    } else if (!node.detail.empty() && node.detail != "-") {
      return StatusError(StatusCode::kNotImplemented,
                         "GoogleSQL AST: unsupported unary operator " +
                             std::string(node.detail));
    }
    return UnaryExpressionExp(std::move(not_e), op);
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
      // Decide array-vs-string concatenation from the evaluated operand type:
      // an ARRAY column is a kColumnValue expression (not a kArrayExp literal),
      // so the old AST-shape-only test sent `array_col || array_col` to
      // string concat. A VarChar element type could be an array of strings or
      // a plain string; ResultType() tells them apart after evaluation.
      bool is_array = arg->Type() == TypeTag::kArrayExp;
      if (!is_array) {
        try {
          is_array = arg->ResultType(Schema()).GetType() == TypeTag::kArray;
        } catch (const std::exception&) {
          // No schema available here for a bare column: fall back to the
          // caller's inferred type (ConcatExpr::InferArrayElementSqlType).
          auto inferred_or = InferArrayElementSqlType(*child);
          const std::string inferred =
              inferred_or.HasValue() ? inferred_or.MoveValue() : std::string{};
          is_array = inferred.starts_with("ARRAY<");
        }
      }
      if (!is_array) {
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
    const std::string accessor_upper = AsciiToUpper(accessor);
    if (fn.find("_safe") == std::string::npos &&
        (accessor_upper.starts_with("SAFE_") ||
         accessor_upper.find("_SAFE") != std::string::npos)) {
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
    clauses.reserve(node.children.size());
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
    clauses.reserve(node.children.size());
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
    operands.reserve(node.children.size());
    for (const auto& child : node.children) {
      if (child->kind != "Location") {
        operands.push_back(child.get());
      }
    }
    if (operands.size() != 3) {
      return AstError<Expression>("GoogleSQL AST: BETWEEN arity");
    }
    // GoogleSQL evaluates X only once. The `X >= lo AND X <= hi` expansion
    // below duplicates X, which changes the result when X is volatile
    // (RAND() BETWEEN 0.3 AND 0.4 drew twice); route that case through
    // __between(X, lo, hi), which evaluates each operand exactly once.
    {
      ASSIGN_OR_RETURN(Expression, probe, (VisitExpression(*operands[0])));
      if (ContainsVolatileCall(probe)) {
        ASSIGN_OR_RETURN(Expression, lo, (VisitExpression(*operands[1])));
        ASSIGN_OR_RETURN(Expression, hi, (VisitExpression(*operands[2])));
        Expression between = FunctionCallExp(
            "__between", {std::move(probe), std::move(lo), std::move(hi)});
        if (node.detail == "NOT BETWEEN") {
          between =
              UnaryExpressionExp(std::move(between), UnaryOperation::kNot);
        }
        return between;
      }
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
      array_query->SetArrayElementSqlType(std::move(h_arr_t));
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
    std::string part;
    if (auto token = WeekdayDatePartToken(*node.children[0])) {
      // EXTRACT(WEEK(SUNDAY) FROM d): a weekday-qualified part spelling,
      // not a plain path; lower it to the part-token form.
      part = std::move(*token);
    } else {
      ASSIGN_OR_RETURN(std::string, hv128166_0, (Path(*node.children[0])));
      part = Lower(std::move(hv128166_0));
    }
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
        const int64_t amount = ParseIntervalAmount(str_val);
        // The IntervalExpression constructor parses eagerly and its EXC-SHIM
        // wrapper THROWS on unparsable text (fuzz-found: INTERVAL '<empty>'
        // with no unit aborted the process).  Reject through Status instead.
        if (!IntervalValue::TryParse(str_val.empty()
                                         ? std::string_view("0")
                                         : std::string_view(str_val),
                                     unit)
                 .HasValue()) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "unsupported INTERVAL literal: '" + str_val + "' " + unit);
        }
        return IntervalExpressionExp(amount, std::move(unit),
                                     std::move(str_val));
      }
      ASSIGN_OR_RETURN(Expression, hv129819_0, (VisitExpression(*value_node)));
      Expression expr = std::move(hv129819_0);
      if (expr->Type() == TypeTag::kConstantValue) {
        const Value& v = expr->AsConstantValue().GetValue();
        if (v.type == ValueType::kInt64) {
          if (!IntervalValue::TryFromInteger(v.value.int_value, unit)
                   .HasValue()) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "unsupported INTERVAL literal: interval constant");
          }
          return IntervalExpressionExp(v.value.int_value, std::move(unit));
        }
        if (v.type == ValueType::kVarChar) {
          std::string str_val = std::string(v.value.varchar_value);
          const int64_t amount = ParseIntervalAmount(str_val);
          if (!IntervalValue::TryParse(str_val.empty()
                                           ? std::string_view("0")
                                           : std::string_view(str_val),
                                       unit)
                   .HasValue()) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "unsupported INTERVAL literal: interval string constant");
          }
          return IntervalExpressionExp(amount, std::move(unit),
                                       std::move(str_val));
        }
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        std::string col_name = expr->AsColumnValue().GetColumnName().name;
        if (HasSessionConstant(col_name)) {
          std::string str_val = GetSessionConstant(col_name);
          if (!IntervalValue::TryParse(
                   (str_val.empty() ? std::string("0") : str_val), unit)
                   .HasValue()) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "unsupported INTERVAL literal: session-constant interval");
          }
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
          if (!IntervalValue::TryFromInteger(v.Value().value.int_value, unit)
                   .HasValue()) {
            return StatusError(StatusCode::kInvalidArgument,
                               "unsupported INTERVAL literal: folded interval");
          }
          return IntervalExpressionExp(v.Value().value.int_value,
                                       std::move(unit));
        }
        if (v.Value().type == ValueType::kVarChar) {
          std::string str_val = std::string(v.Value().value.varchar_value);
          const int64_t amount = ParseIntervalAmount(str_val);
          if (!IntervalValue::TryParse(str_val.empty()
                                           ? std::string_view("0")
                                           : std::string_view(str_val),
                                       unit)
                   .HasValue()) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "unsupported INTERVAL literal: folded interval string");
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
        // A bare 9223372036854775808 is a UINT64 literal: only its negation
        // (-9223372036854775808, a unary-minus node, not this IntLiteral
        // child) spells INT64_MIN, so narrowing it to INT64 is out of range.
        const std::string upper_target = UpperCopy(target);
        // Widening targets accept the full UINT64 range. Let the general cast
        // path fold the UINT64-typed literal (see the IntLiteral visitor)
        // into STRING/FLOAT64/DOUBLE instead of rejecting it here.
        if (upper_target == "STRING" || upper_target == "FLOAT64" ||
            upper_target == "DOUBLE" || upper_target == "JSON") {
          // fall through to the normal CAST construction below
        } else if (safe_cast_target) {
          return ConstantValueExp(Value());
        } else {
          return AstError<Expression>(upper_target +
                                      " out of range: " + digits);
        }
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
    if (const Status status =
            ValidateStructCastCoercibility(node, safe_cast_target);
        status != Status::kSuccess) {
      if (safe_cast_target) {
        return ConstantValueExp(Value());
      }
      return status;
    }
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
    fields.reserve(node.children.size());
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
        if (val_expr && ContainsVolatileCall(val_expr)) {
          // A volatile field (RAND(), UUID(), ...) must be drawn per row.
          // Folding it at visit time would freeze one value for every row and
          // for every template replay; defer it to runtime like an
          // unevaluable aggregate/subquery instead.
          field.deferred = val_expr;
        } else if (val_expr) {
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
              } else if (v.type ==
                         ValueType::kInt64) {  // NOLINT(bugprone-branch-clone)
                // AsString() respects the unsigned tag; to_string(int_value)
                // would render UINT64_MAX as -1 in the folded struct text.
                field.text = v.AsString();
              } else if (v.type == ValueType::kDouble) {
                field.text = FormatDoubleShortest(v.value.double_value);
              } else if (v.type == ValueType::kDate) {
                field.text = v.AsString();
                field.is_string = true;
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
      // flag arguments, resolved by the function evaluator per row or per
      // group.
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
    std::string json_out = "{";
    bool first = true;
    // JSON string escaping shared by field names and string values: an
    // unescaped quote or backslash inside a value (e.g. STRUCT('a"b')) made
    // the folded struct JSON unparsable.
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
          // No short escapes for \n/\r/\t: the runtime EncodeStructJson()
          // emits \u00XX for every control char, and struct values are
          // compared as raw JSON text. Emitting "\n" here would make the
          // folded and runtime forms of the same STRUCT compare unequal.
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
    // Collation propagation: when any field value carries a case-insensitive
    // collator, the whole struct comparison folds (GoogleSQL resolves one
    // collation per comparison; struct JSON carries no per-field metadata).
    // Fold at comparison time via the value's collation tag instead of
    // lowercasing the field text here: projections and TO_JSON_STRING must
    // keep the literal's original case.
    Value struct_value(std::move(json_out));
    if (any_ci) {
      struct_value.SetCollation(
          static_cast<uint8_t>(Collation::kCaseInsensitive));
    }
    return ConstantValueExp(std::move(struct_value));
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

}  // namespace ast_visitor_detail
bool NeedsRelationalEvaluation(
    const Expression&
        expression,  // NOLINT(misc-no-recursion) // AST traversal recursion is
                     // intentional; expression depth bounded by
                     // ExpressionDepthGuard (kMaxExpressionDepth).
    bool top_level) {
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
      const auto& call = expression->AsFunctionCallExpression();
      // The star form of the value-table shim is transparent to every
      // evaluator; the other proto plumbing forms still need the relational
      // interpreter's proto-field resolution.
      if (call.FuncName() == "__value_table_value") {
        return call.Args().size() != 1 || !call.Args()[0] ||
               call.Args()[0]->Type() != TypeTag::kColumnValue ||
               call.Args()[0]->AsColumnValue().GetColumnName().name != "*";
      }
      if (call.FuncName() == "__proto_new") {
        return true;
      }
      std::string lower_name = call.FuncName();
      std::ranges::transform(
          lower_name, lower_name.begin(),
          [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
      // A SQL scalar UDF's body evaluates through the interpreter's scope
      // chain; it stays relational regardless of the hatch below.
      if (FindSqlScalarFunction(lower_name).has_value()) {
        return true;
      }
      // Ordinary scalar functions (EXTRACT, UPPER, SUBSTR, ...) evaluate the
      // same through the plan/AST evaluators.  The 2026-10-03 hatch lets a
      // GROUP BY key / SELECT item / WHERE term carrying them cross the
      // Cascades bridge instead of dragging the whole statement onto the
      // relational route.  Default off: flipping it needs the TPC-H Q9 A/B
      // (the 2026-10-03 round-8 revert) plus the full TPC-DS/TPC-H suite;
      // the estimate collapse that regressed Q9 then (vacuous stats on
      // computed projection columns) was fixed the same day in
      // ProjectionPlan/table_statistics.
      // Even when the hatch lets a scalar call cross the Cascades bridge,
      // its arguments may contain subqueries/relations that still need the
      // relational route (same recursion every other node kind performs).
      for (const Expression& arg : call.Args()) {
        // Every other composite node recurses with top_level=false; a
        // function argument is nested too. Passing true here would treat
        // ABS(COUNT(x))'s inner aggregate as top-level and skip the
        // relational route, diverging from -COUNT(x).
        if (NeedsRelationalEvaluation(arg, false)) {
          return true;
        }
      }
      static const bool scalar_functions_cascades = [] {
        const char* hatch = std::getenv("TINYLAMB_SCALAR_FN_CASCADES");
        return hatch != nullptr && std::string_view(hatch) == "1";
      }();
      return !scalar_functions_cascades;
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
      if (name.name == "*") {
        return false;
      }
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
StatusOr<std::string> DecodeStringEscapes(std::string_view value, bool is_bytes,
                                          bool is_triple, char quote) {
  return DecodeStringEscapesImpl(value, is_bytes, is_triple, quote);
}
std::optional<std::string> NormalizeTimestampText(const std::string& text) {
  return NormalizeTimestampTextImpl(text);
}

}  // namespace tinylamb
