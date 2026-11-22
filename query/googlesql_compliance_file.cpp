/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/googlesql_compliance_file.hpp"

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
#include <filesystem>
#include <limits>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/string_util.hpp"
#include "type/date.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

std::vector<std::string> SplitTopLevel(std::string_view text);

std::string Trim(std::string_view text) {
  size_t begin = 0;
  while (begin < text.size() &&
         std::isspace(static_cast<unsigned char>(text[begin])) != 0) {
    ++begin;
  }
  size_t end = text.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(text[end - 1])) != 0) {
    --end;
  }
  return std::string(text.substr(begin, end - begin));
}

std::string ToLower(std::string text) {
  std::ranges::transform(text, text.begin(),
                         [](char c) { return AsciiToLower(c); });
  return text;
}

bool LineIsSeparator(std::string_view line, std::string_view expected) {
  return Trim(line) == expected;
}

// Byte offset of an " as " separator that sits outside parentheses, brackets,
// angle brackets and quoted literals, or npos when none exists.
size_t TopLevelAsPosition(const std::string& lower) {
  const std::string needle = " as ";
  int depth = 0;
  bool in_string = false;
  char quote = '\0';
  for (size_t i = 0; i < lower.size(); ++i) {
    const char c = lower[i];
    if (in_string) {
      if (c == '\\' && i + 1 < lower.size()) {
        ++i;
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
      continue;
    }
    if (c == '[' || c == '{' || c == '(' || c == '<') {
      ++depth;
    } else if (c == ']' || c == '}' || c == ')' || c == '>') {
      if (depth > 0) {
        --depth;
      }
    } else if (c == ' ' && depth == 0 &&
               lower.compare(i, needle.size(), needle) == 0) {
      return i;
    }
  }
  return std::string::npos;
}

// True when `s` ends with "<whitespace><identifier>" and the boundary sits at
// bracket depth zero outside string literals.
bool SplitTrailingIdentifier(const std::string& s, size_t* split) {
  if (s.empty()) {
    return false;
  }
  size_t end = s.size();
  while (end > 0 &&
         (std::isalnum(static_cast<unsigned char>(s[end - 1])) != 0 ||
          s[end - 1] == '_')) {
    --end;
  }
  // The identifier must be preceded by whitespace *and* by a non-empty
  // expression. `end == 1` leaves no expression before the separator (and
  // would make the loop bound `end - 1` zero), so reject it explicitly.
  if (end == s.size() || end < 2) {
    return false;
  }
  // The identifier must be preceded by whitespace...
  if (std::isspace(static_cast<unsigned char>(s[end - 1])) == 0) {
    return false;
  }
  // ...at bracket depth zero outside strings.
  int depth = 0;
  bool in_string = false;
  char quote = '\0';
  for (size_t i = 0; i < end - 1; ++i) {
    const char c = s[i];
    if (in_string) {
      if (c == '\\' && i + 1 < end - 1) {
        ++i;
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
      continue;
    }
    if (c == '[' || c == '{' || c == '(' || c == '<') {
      ++depth;
    }
    if (c == ']' || c == '}' || c == ')' || c == '>') {
      --depth;
    }
  }
  if (depth != 0 || in_string) {
    return false;
  }
  *split = end;
  return true;
}

std::vector<std::string> SplitCases(std::string_view contents) {
  std::vector<std::string> cases;
  std::string current;
  std::string line;
  std::istringstream stream{std::string(contents)};
  while (std::getline(stream, line)) {
    if (LineIsSeparator(line, "==")) {
      cases.push_back(std::move(current));
      current.clear();
      continue;
    }
    current += line;
    current.push_back('\n');
  }
  if (!Trim(current).empty()) {
    cases.push_back(std::move(current));
  }
  return cases;
}

// ZetaSQL compliance option keys tinylamb does not implement.  They are part
// of the upstream option vocabulary, so they are consumed (and ignored); a
// leading "default " (the file-default spelling) is stripped before matching.
// Anything outside this set and the handled keys above is not an option line.
constexpr auto kIgnoredOptionKeys = std::to_array<std::string_view>({
    "labels",
    "extract_labels",
    "global_labels",
    "load_proto_files",
    "load_proto_names",
    "load_enum_names",
    "prepare_database_additional_features",
    "skip_required_feature_integrity_check",
    "exclude_in_sql_builder_pipe_sql_equivalence_tests",
    "use_test_database_copy",
    "reserve_match_recognize",
    "reserve_graph_table",
});

bool ConsumeOptionLine(std::string_view line, GoogleSqlComplianceCase* out,
                       std::vector<std::string>* file_default_features,
                       bool* file_default_time_zone) {
  const std::string trimmed = Trim(line);
  if (trimmed.size() < 2 || trimmed.front() != '[' || trimmed.back() != ']') {
    return false;
  }
  const std::string body = trimmed.substr(1, trimmed.size() - 2);
  const size_t eq = body.find('=');
  const std::string key =
      ToLower(Trim(eq == std::string::npos ? body : body.substr(0, eq)));
  const std::string value =
      eq == std::string::npos ? std::string() : Trim(body.substr(eq + 1));
  if (key == "name") {
    out->name = value;
    return true;
  }
  if (key == "prepare_database") {
    out->prepare_database = true;
    return true;
  }
  if (key == "required_features" || key == "required_feature") {
    std::string feature;
    std::istringstream features(value);
    while (std::getline(features, feature, ',')) {
      feature = Trim(feature);
      if (!feature.empty()) {
        out->required_features.push_back(feature);
      }
    }
    return true;
  }
  if (key == "forbidden_features" || key == "forbidden_feature") {
    // The case is only valid while the engine LACKS every listed capability
    // (reference-driver contract); the runner gates on the parsed set.
    std::string feature;
    std::istringstream features(value);
    while (std::getline(features, feature, ',')) {
      feature = Trim(feature);
      if (!feature.empty()) {
        out->forbidden_features.push_back(feature);
      }
    }
    return true;
  }
  if (key == "default required_features" || key == "default required_feature") {
    file_default_features->clear();
    std::string feature;
    std::istringstream features(value);
    while (std::getline(features, feature, ',')) {
      feature = Trim(feature);
      if (!feature.empty()) {
        file_default_features->push_back(feature);
        // The declaration inside a segment also gates that segment itself.
        out->required_features.push_back(feature);
      }
    }
    return true;
  }
  if (key == "primary_key_mode") {
    out->primary_key_mode = value;
    return true;
  }
  if (key == "secondary_index") {
    out->secondary_indexes.push_back(Trim(value));
    return true;
  }
  if (key == "mode") {
    out->mode = ToLower(Trim(value));
    return true;
  }
  if (key == "plan_contains") {
    out->plan_contains.push_back(value);
    return true;
  }
  if (key == "plan_not_contains") {
    out->plan_not_contains.push_back(value);
    return true;
  }
  if (key == "parameters" || key == "parameter") {
    for (const auto& item : SplitTopLevel(value)) {
      const std::string s = Trim(item);
      const std::string lower = ToLower(s);
      const size_t as_pos = TopLevelAsPosition(lower);
      if (as_pos != std::string::npos) {
        const std::string expr = Trim(s.substr(0, as_pos));
        const std::string name = Trim(s.substr(as_pos + 4));
        if (!name.empty() && !expr.empty()) {
          out->parameters.emplace_back(name, expr);
          continue;
        }
      }
      // Compact "<value> <name>" form (e.g. "1 param", "NULL param").  A
      // trailing identifier wins over any embedded " AS ": expressions such
      // as `cast(1 as int32) a` carry their own AS clause, so the separator
      // search must not see it.
      size_t split = 0;
      if (SplitTrailingIdentifier(s, &split)) {
        const std::string expr = Trim(s.substr(0, split));
        const std::string name = Trim(s.substr(split));
        if (!name.empty() && !expr.empty()) {
          out->parameters.emplace_back(name, expr);
        }
        continue;
      }
      // Neither "<expr> AS <name>" nor "<expr> <name>" parsed: the item would
      // be dropped silently, leaving a literal @name unsubstituted in the
      // SQL. Record it so the runner can fail the case instead of passing on
      // an unbound parameter.
      out->unparseable_parameters.push_back(s);
    }
    return true;
  }
  if (key == "default_time_zone") {
    // Case-local: applies only to the query it is part of.
    out->default_time_zone = value;
    return true;
  }
  if (key == "default default_time_zone") {
    // File-level default: the caller propagates it to later segments.
    out->default_time_zone = value;
    *file_default_time_zone = true;
    return true;
  }
  std::string_view bare_key = key;
  if (bare_key.starts_with("default ")) {
    bare_key.remove_prefix(std::string_view("default ").size());
  }
  if (bare_key == "unknown_order") {
    // The order-unspecified declaration must flip the comparison flag; the
    // upstream vocabulary spells it bare (`[unknown_order]` = true) or with
    // an explicit value. Absorbing it into kIgnoredOptionKeys left the
    // harness comparing strict row order against `-- unknown order` output.
    const std::string lower_value = ToLower(Trim(value));
    out->unknown_order =
        value.empty() || lower_value == "true" || lower_value == "1";
    return true;
  }
  return std::ranges::any_of(
      kIgnoredOptionKeys,
      [&](const std::string_view ignored) { return bare_key == ignored; });
}

bool ParseStructRow(std::string_view text, size_t* offset,
                    std::vector<std::string>* fields) {
  while (*offset < text.size() &&
         std::isspace(static_cast<unsigned char>(text[*offset])) != 0) {
    ++*offset;
  }
  if (*offset >= text.size() || text[*offset] != '{') {
    return false;
  }
  ++*offset;
  std::string token;
  token.reserve(text.size());
  int depth = 1;
  int bracket_depth = 0;
  bool in_string = false;
  char quote = '\0';
  auto flush = [&]() {
    const std::string field = Trim(token);
    if (!field.empty()) {
      fields->push_back(field);
    }
    token.clear();
  };
  while (*offset < text.size() && depth > 0) {
    const char c = text[*offset];
    ++*offset;
    if (in_string) {
      token.push_back(c);
      if (c == '\\' && *offset < text.size()) {
        token.push_back(text[*offset]);
        ++*offset;
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
      token.push_back(c);
      continue;
    }
    if (c == '{') {
      ++depth;
      token.push_back(c);
      continue;
    }
    if (c == '}') {
      --depth;
      if (depth == 0) {
        flush();
        return true;
      }
      token.push_back(c);
      continue;
    }
    if (c == '[' || c == '(' || c == '<') {
      ++bracket_depth;
      token.push_back(c);
      continue;
    }
    if (c == ']' || c == ')' || c == '>') {
      if (bracket_depth > 0) {
        --bracket_depth;
      }
      token.push_back(c);
      continue;
    }
    if (c == ',' && depth == 1 && bracket_depth == 0) {
      flush();
      continue;
    }
    token.push_back(c);
  }
  return false;
}

void ParseExpectedRows(std::string_view result, GoogleSqlComplianceCase* out) {
  // An `[unknown_order]` option line may already have set the flag; the
  // `unknown order:` phrase in the expectation is the other spelling. It is
  // only honored at the ordering-annotation slot (right where an element
  // would start inside the top-level '['), never as a substring of the
  // payload — a VARCHAR golden containing the words must not flip the
  // comparison to unordered.
  const size_t array_open = result.find('[');
  if (array_open == std::string_view::npos) {
    return;
  }
  size_t offset = array_open + 1;
  while (offset < result.size()) {
    while (offset < result.size() &&
           std::isspace(static_cast<unsigned char>(result[offset])) != 0) {
      ++offset;
    }
    if (offset >= result.size() || result[offset] == ']') {
      break;
    }
    if (result[offset] == ',') {
      ++offset;
      continue;
    }
    // Skip an ordering annotation before the first element.
    const std::string lower_rest =
        ToLower(std::string(result.substr(offset, 14)));
    if (lower_rest.starts_with("unknown order:")) {
      out->unknown_order = true;
      offset += 14;
    } else if (lower_rest.starts_with("known order:")) {
      offset += 12;
    }
    while (offset < result.size() &&
           std::isspace(static_cast<unsigned char>(result[offset])) != 0) {
      ++offset;
    }
    if (offset >= result.size() || result[offset] == ']') {
      break;
    }
    std::vector<std::string> fields;
    if (result[offset] == '{') {
      if (!ParseStructRow(result, &offset, &fields)) {
        break;
      }
      out->expected_rows.push_back(std::move(fields));
      continue;
    }
    // Bare scalar element (e.g. ARRAY<INT64>[10, 20] or value-table rows):
    // one row per comma-separated token until the closing bracket.
    size_t end = offset;
    int depth = 0;
    bool in_string = false;
    char quote = '\0';
    while (end < result.size()) {
      const char c = result[end];
      if (in_string) {
        if (c == '\\' && end + 1 < result.size()) {
          ++end;
        } else if (c == quote) {
          in_string = false;
        }
        ++end;
        continue;
      }
      if (c == '"' || c == '\'') {
        in_string = true;
        quote = c;
        ++end;
        continue;
      }
      if (c == '<' || c == '[' || c == '(' || c == '{') {
        ++depth;
      } else if ((c == '>' || c == ')' || c == '}' || c == ']') && depth > 0) {
        --depth;
      } else if ((c == ',' || c == ']') && depth == 0) {
        break;
      }
      ++end;
    }
    const std::string token = Trim(result.substr(offset, end - offset));
    offset = end;
    if (!token.empty()) {
      out->expected_rows.push_back({token});
    }
  }
}

void ParseResult(std::string_view result, GoogleSqlComplianceCase* out) {
  out->raw_result = std::string(result);
  const std::string trimmed = Trim(result);
  if (trimmed.size() >= 6 && ToLower(trimmed.substr(0, 6)) == "error:") {
    out->expect_error = true;
    out->error_text = Trim(trimmed.substr(6));
    return;
  }
  ParseExpectedRows(result, out);
}

// The compliance corpus escapes comment markers on continuation lines
// ("\-- text"); the reference parser treats them as plain "--" comments.
std::string NormalizeCommentEscapes(std::string sql) {
  size_t pos = 0;
  while ((pos = sql.find("\\--", pos)) != std::string::npos) {
    sql.erase(pos, 1);
    pos += 2;
  }
  return sql;
}

std::string ApplyParameters(
    std::string sql,
    const std::vector<std::pair<std::string, std::string>>& parameters) {
  if (parameters.empty()) {
    return sql;
  }
  auto is_ident = [](char c) {
    return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_';
  };
  std::string out;
  out.reserve(sql.size());
  const size_t n = sql.size();
  size_t i = 0;
  while (i < n) {
    const char c = sql[i];
    // Comments and quoted regions are copied verbatim: a parameter-looking
    // token inside them must never be rewritten.
    if (c == '-' && i + 1 < n && sql[i + 1] == '-') {
      const size_t eol = sql.find('\n', i);
      const size_t end = eol == std::string::npos ? n : eol;
      out.append(sql, i, end - i);
      i = end;
      continue;
    }
    if (c == '/' && i + 1 < n && sql[i + 1] == '*') {
      const size_t close = sql.find("*/", i + 2);
      const size_t end = close == std::string::npos ? n : close + 2;
      out.append(sql, i, end - i);
      i = end;
      continue;
    }
    if (c == '\'' || c == '"' || c == '`') {
      const char quote = c;
      size_t j = i + 1;
      while (j < n) {
        if (sql[j] == '\\' && j + 1 < n) {
          j += 2;
          continue;
        }
        if (sql[j] == quote) {
          if (j + 1 < n && sql[j + 1] == quote) {
            j += 2;  // doubled-quote escape
            continue;
          }
          ++j;
          break;
        }
        ++j;
      }
      out.append(sql, i, j - i);
      i = j;
      continue;
    }
    bool replaced = false;
    if (c == '@' && (i == 0 || (!is_ident(sql[i - 1]) && sql[i - 1] != '@'))) {
      for (const auto& [name, expr] : parameters) {
        if (name.empty()) {
          continue;
        }
        const std::string target = "@" + name;
        if (sql.compare(i, target.size(), target) == 0 &&
            (i + target.size() >= n || !is_ident(sql[i + target.size()]))) {
          out += '(';
          out += expr;
          out += ')';
          i += target.size();
          replaced = true;
          break;
        }
      }
    }
    if (!replaced) {
      out += c;
      ++i;
    }
  }
  return out;
}

// Option blocks such as multi-line `[parameters=...]` span several physical
// lines until their brackets balance (quotes ignored). Joins continuation
// lines into one logical line so ConsumeOptionLine can parse the block.
std::string ReadLogicalOptionLine(std::istringstream* stream,
                                  std::string first_line) {
  auto unbalanced = [](const std::string& text) {
    int depth = 0;
    bool in_string = false;
    char quote = '\0';
    for (size_t i = 0; i < text.size(); ++i) {
      const char c = text[i];
      if (in_string) {
        if (c == '\\' && i + 1 < text.size()) {
          ++i;
        } else if (c == quote) {
          in_string = false;
        }
        continue;
      }
      if (c == '"' || c == '\'') {
        in_string = true;
        quote = c;
      } else if (c == '[' || c == '(' || c == '<') {
        ++depth;
      } else if (c == ']' || c == ')' || c == '>') {
        --depth;
      }
    }
    return depth > 0;
  };
  std::string line = std::move(first_line);
  while (unbalanced(line)) {
    std::string next;
    if (!std::getline(*stream, next)) {
      break;
    }
    line.push_back(' ');
    line += next;
  }
  return line;
}

GoogleSqlComplianceCase ParseSegment(
    std::string_view file, std::string_view segment,
    std::string* current_default_tz,
    std::vector<std::string>* default_features) {
  GoogleSqlComplianceCase test_case;
  test_case.file = std::string(file);
  test_case.default_time_zone = *current_default_tz;
  test_case.required_features = *default_features;
  std::istringstream stream{std::string(segment)};
  std::string line;
  bool in_options = true;
  std::string sql;
  while (std::getline(stream, line)) {
    const std::string trimmed = Trim(line);
    if (in_options) {
      if (trimmed.empty() || trimmed.starts_with('#')) {
        continue;
      }
      if (trimmed.front() == '[') {
        line = ReadLogicalOptionLine(&stream, line);
      }
      bool file_default_time_zone = false;
      if (ConsumeOptionLine(line, &test_case, default_features,
                            &file_default_time_zone)) {
        if (file_default_time_zone && !test_case.default_time_zone.empty()) {
          *current_default_tz = test_case.default_time_zone;
        }
        continue;
      }
      in_options = false;
    }
    if (LineIsSeparator(line, "--")) {
      if (sql.empty()) {
        // A '--' before any query text cannot separate query from results;
        // upstream corpus carries a stray one (struct_queries.test
        // struct_equality_different_field_names). Skip it so the real query
        // still parses instead of fabricating an empty-SQL case.
        continue;
      }
      std::ostringstream rest;
      rest << stream.rdbuf();
      ParseResult(rest.str(), &test_case);
      test_case.sql = NormalizeCommentEscapes(
          ApplyParameters(Trim(sql), test_case.parameters));
      if (test_case.name.empty()) {
        test_case.name =
            test_case.prepare_database ? "prepare_database" : "unnamed";
      }
      return test_case;
    }
    if (sql.empty()) {
      if (trimmed.empty() || trimmed.starts_with('#')) {
        continue;
      }
      if (trimmed.front() == '[') {
        line = ReadLogicalOptionLine(&stream, line);
      }
      bool file_default_time_zone = false;
      if (ConsumeOptionLine(line, &test_case, default_features,
                            &file_default_time_zone)) {
        if (file_default_time_zone && !test_case.default_time_zone.empty()) {
          *current_default_tz = test_case.default_time_zone;
        }
        continue;
      }
    }
    sql += line;
    sql.push_back('\n');
  }
  test_case.sql =
      NormalizeCommentEscapes(ApplyParameters(Trim(sql), test_case.parameters));
  if (test_case.name.empty()) {
    test_case.name =
        test_case.prepare_database ? "prepare_database" : "unnamed";
  }
  return test_case;
}

std::string Unquote(std::string_view token) {
  // A bytes prefix is only a prefix when it is immediately followed by a
  // quote.  Stripping a leading 'b' from a bare token such as `banana` would
  // silently corrupt the golden (`anana`).
  if (token.size() >= 2 && (token.front() == 'b' || token.front() == 'B') &&
      (token[1] == '"' || token[1] == '\'')) {
    token.remove_prefix(1);
  }
  if (token.size() >= 2 && ((token.front() == '"' && token.back() == '"') ||
                            (token.front() == '\'' && token.back() == '\''))) {
    std::string out;
    out.reserve(token.size());
    for (size_t i = 1; i + 1 < token.size(); ++i) {
      if (token[i] == '\\' && i + 2 < token.size()) {
        char next = token[++i];
        if (next == 'x' && i + 2 < token.size()) {
          const std::string hex_str = std::string(token.substr(i + 1, 2));
          size_t consumed = 0;
          try {
            const int val = std::stoi(hex_str, &consumed, 16);
            // std::stoi accepts a numeric prefix ("1g" parses as 1).  A
            // well-formed \x escape has exactly two hex digits and fits in a
            // byte; anything else stays literal instead of injecting garbage.
            if (consumed == hex_str.size() && val >= 0 && val <= 0xFF) {
              out.push_back(static_cast<char>(val));
              i += 2;
            } else {
              out.push_back(next);
            }
          } catch (...) {
            out.push_back(next);
          }
        } else if (next == '0') {
          out.push_back('\0');
        } else if (next == 'n') {
          out.push_back('\n');
        } else if (next == 't') {
          out.push_back('\t');
        } else if (next == 'r') {
          out.push_back('\r');
        } else {
          out.push_back(next);
        }
        continue;
      }
      out.push_back(token[i]);
    }
    return out;
  }
  return std::string(token);
}

std::vector<std::string> SplitTopLevel(std::string_view text) {
  std::vector<std::string> parts;
  parts.reserve(text.size());
  std::string current;
  current.reserve(text.size());
  int depth = 0;
  bool in_string = false;
  char quote = '\0';
  for (size_t i = 0; i < text.size(); ++i) {
    const char c = text[i];
    if (in_string) {
      current.push_back(c);
      if (c == '\\' && i + 1 < text.size()) {
        current.push_back(text[++i]);
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
    if (c == '[' || c == '{' || c == '(' || c == '<') {
      ++depth;
      current.push_back(c);
      continue;
    }
    if (c == ']' || c == '}' || c == ')' || c == '>') {
      if (depth > 0) {
        --depth;
      }
      current.push_back(c);
      continue;
    }
    if (c == ',' && depth == 0) {
      const std::string part = Trim(current);
      if (!part.empty()) {
        parts.push_back(part);
      }
      current.clear();
      continue;
    }
    current.push_back(c);
  }
  const std::string part = Trim(current);
  if (!part.empty()) {
    parts.push_back(part);
  }
  return parts;
}

bool ParseArrayToken(std::string_view token, std::string* sql_type,
                     std::vector<std::string>* elements,
                     bool* unordered = nullptr) {
  const std::string text = Trim(token);
  constexpr std::string_view kPrefix = "ARRAY<";
  if (text.size() < kPrefix.size() || !text.starts_with(kPrefix)) {
    return false;
  }
  int depth = 1;
  size_t close = std::string::npos;
  for (size_t i = kPrefix.size(); i < text.size(); ++i) {
    if (text[i] == '<') {
      ++depth;
    } else if (text[i] == '>') {
      --depth;
      if (depth == 0) {
        close = i;
        break;
      }
    }
  }
  if (close == std::string::npos) {
    return false;
  }
  *sql_type = Trim(text.substr(kPrefix.size(), close - kPrefix.size()));
  std::string rest = Trim(text.substr(close + 1));
  if (rest.size() < 2 || rest.front() != '[' || rest.back() != ']') {
    return false;
  }
  std::string inner = Trim(rest.substr(1, rest.size() - 2));
  const std::string lower = ToLower(inner);
  constexpr std::string_view kKnown = "known order:";
  constexpr std::string_view kUnknown = "unknown order:";
  if (lower.starts_with(kKnown)) {
    inner = Trim(inner.substr(kKnown.size()));
  } else if (lower.starts_with(kUnknown)) {
    inner = Trim(inner.substr(kUnknown.size()));
    if (unordered != nullptr) {
      *unordered = true;
    }
  }
  *elements = SplitTopLevel(inner);
  return true;
}

std::string QuoteComplianceString(std::string_view text, bool bytes) {
  std::string out = bytes ? "b\"" : "\"";
  out.reserve(text.size());
  for (const char c : text) {
    if (c == '"' || c == '\\') {
      out.push_back('\\');
    }
    out.push_back(c);
  }
  out.push_back('"');
  return out;
}

}  // namespace

std::vector<GoogleSqlComplianceCase> ParseGoogleSqlComplianceFile(
    std::string_view path, std::string_view contents) {
  const std::string file =
      std::filesystem::path(std::string(path)).filename().string();
  std::vector<GoogleSqlComplianceCase> cases;
  size_t unnamed = 0;
  std::string current_default_tz = "America/Los_Angeles";
  std::vector<std::string> default_features;
  for (const std::string& segment : SplitCases(contents)) {
    if (Trim(segment).empty()) {
      continue;
    }
    GoogleSqlComplianceCase test_case =
        ParseSegment(file, segment, &current_default_tz, &default_features);
    if (test_case.name == "unnamed" ||
        test_case.name.starts_with("prepare_database")) {
      if (test_case.name.find('_') == std::string::npos ||
          test_case.name == "unnamed" || test_case.name == "prepare_database") {
        test_case.name += "_" + std::to_string(unnamed++);
      }
    }
    if (test_case.sql.empty() && !test_case.prepare_database) {
      // A segment can carry an expected result yet no SQL (a stray '--'
      // separator, or a malformed entry). Dropping it silently lets a whole
      // expectation vanish; keep any case that still has a result or an
      // expected error so the runner fails it loudly. Plan-assertion-only
      // stubs (the optimizer fixtures' "Future:" placeholders) carry no
      // executable expectation and stay dropped.
      const bool verifiable =
          !test_case.expected_rows.empty() || test_case.expect_error;
      if (!verifiable) {
        continue;
      }
    }
    cases.push_back(std::move(test_case));
  }
  return cases;
}

std::vector<std::string> ListGoogleSqlComplianceFiles(
    std::string_view directory) {
  std::vector<std::string> files;
  std::error_code error;
  for (const auto& entry :
       std::filesystem::directory_iterator(std::string(directory), error)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    if (entry.path().extension() != ".test") {
      continue;
    }
    files.push_back(entry.path().filename().string());
  }
  std::ranges::sort(files);
  return files;
}

bool IsDifferentialPrivacyCase(const GoogleSqlComplianceCase& test_case) {
  const std::string hay =
      ToLower(test_case.file + " " + test_case.name + " " + test_case.sql);
  if (hay.find("differential_privacy") != std::string::npos) {
    return true;
  }
  if (hay.find("differential privacy") != std::string::npos) {
    return true;
  }
  if (hay.find("anonymization") != std::string::npos) {
    return true;
  }
  if (hay.find("anon_") != std::string::npos) {
    return true;
  }
  return std::ranges::any_of(
      test_case.required_features, [](const std::string& feature) {
        const std::string lower = ToLower(feature);
        return lower.find("anonym") != std::string::npos ||
               lower.find("differential_privacy") != std::string::npos ||
               lower.find("privacy") != std::string::npos;
      });
}

std::string FormatComplianceValue(const Value& value) {
  if (value.IsNull()) {
    return "NULL";
  }
  switch (value.type) {
    case ValueType::kInt64:
      return std::to_string(value.value.int_value);
    case ValueType::kDouble: {
      if (std::isnan(value.value.double_value)) {
        return "nan";
      }
      if (std::isinf(value.value.double_value)) {
        return value.value.double_value > 0 ? "inf" : "-inf";
      }
      std::array<char, 64> buffer{};
      auto [ptr, ec] =
          std::to_chars(buffer.begin(), buffer.end(), value.value.double_value);
      return std::string{buffer.data(),
                         static_cast<size_t>(ptr - buffer.data())};
    }
    case ValueType::kVarChar:
      return std::string(value.value.varchar_value);
    case ValueType::kDate:
      return FormatDateDays(value.value.int_value);
    case ValueType::kArray: {
      const std::string& sql_type = value.ArrayElementSqlType();
      const auto& elements = value.ArrayElements();
      std::string inner;
      inner.reserve(elements.size());
      if (elements.size() >= 2) {
        inner = "known order:";
      }
      for (size_t i = 0; i < elements.size(); ++i) {
        if (i != 0) {
          inner += ", ";
        }
        const Value& element = elements[i];
        if (element.IsNull()) {
          inner += "NULL";
        } else if (sql_type == "BOOL" || sql_type == "BOOLEAN") {
          inner +=
              element.type == ValueType::kInt64 && element.value.int_value != 0
                  ? "true"
                  : "false";
        } else if (sql_type == "STRING") {
          inner += QuoteComplianceString(
              std::string(element.value.varchar_value), false);
        } else if (sql_type == "BYTES") {
          inner += QuoteComplianceString(
              std::string(element.value.varchar_value), true);
        } else {
          inner += FormatComplianceValue(element);
        }
      }
      return "ARRAY<" + sql_type + ">[" + inner + "]";
    }
    case ValueType::kNull:
      return "NULL";
  }
  return value.AsString();
}

namespace {

// Case-insensitive identifier equality for proto member splitting.
bool ProtoIsIdentStart(char c) {
  return std::isalpha(static_cast<unsigned char>(c)) != 0 || c == '_' ||
         c == '[';
}

// Collapses whitespace runs to single spaces (dropping space before closing
// brackets); used to normalize golden text before comparison.
std::string NormalizeWsText(std::string_view str) {
  std::string out;
  out.reserve(str.size());
  bool in_space = false;
  for (char c : str) {
    if (std::isspace(static_cast<unsigned char>(c)) != 0) {
      if (!in_space && !out.empty() && out.back() != '[' && out.back() != '(' &&
          out.back() != '{' && out.back() != ',') {
        out.push_back(' ');
      }
      in_space = true;
    } else {
      if (in_space && !out.empty() &&
          (c == ']' || c == ')' || c == '}' || c == ',')) {
        if (out.back() == ' ') {
          out.pop_back();
        }
      }
      in_space = false;
      out.push_back(c);
    }
  }
  if (!out.empty() && out.back() == ' ') {
    out.pop_back();
  }
  return out;
}

// Splits a proto TEXT / struct body into top-level (name, value) members.
// Members separate on ',', ';' or on whitespace immediately before another
// "identifier:" pair; bracketed groups and quoted strings stay intact.
void SplitProtoMembers(const std::string& body,
                       std::vector<std::pair<std::string, std::string>>* out) {
  int depth = 0;
  bool in_string = false;
  char quote = '\0';
  size_t start = 0;
  const size_t n = body.size();
  auto flush = [&](size_t end) {
    std::string piece = Trim(body.substr(start, end - start));
    if (!piece.empty()) {
      const size_t colon = piece.find(':');
      // Only treat the first top-level colon as a name separator when the
      // prefix is identifier-shaped.  An empty value (`field:` at the end of
      // the piece) is valid: `colon + 1 <= piece.size()` allows it, unlike
      // `colon + 1 <= piece.size() - 1`.
      bool ident = colon != std::string::npos && colon > 0 &&
                   colon + 1 <= piece.size() && ProtoIsIdentStart(piece[0]);
      for (size_t k = 0; ident && k < colon; ++k) {
        const char c = piece[k];
        if (std::isalnum(static_cast<unsigned char>(c)) == 0 && c != '_' &&
            c != '.' && c != '[' && c != ']') {
          ident = false;
          break;
        }
      }
      if (ident) {
        out->emplace_back(ToLower(Trim(piece.substr(0, colon))),
                          Trim(piece.substr(colon + 1)));
      } else {
        // Message fields use `field { ... }` / `field < ... >` syntax and
        // therefore have no colon before their nested body.  Keep the field
        // name so recursive proto comparison can distinguish them from
        // positional STRUCT values.
        size_t name_end = 0;
        while (
            name_end < piece.size() &&
            (std::isalnum(static_cast<unsigned char>(piece[name_end])) != 0 ||
             piece[name_end] == '_' || piece[name_end] == '.' ||
             piece[name_end] == '[' || piece[name_end] == ']')) {
          ++name_end;
        }
        size_t body_start = name_end;
        while (body_start < piece.size() &&
               std::isspace(static_cast<unsigned char>(piece[body_start])) !=
                   0) {
          ++body_start;
        }
        if (name_end > 0 && body_start < piece.size() &&
            (piece[body_start] == '{' || piece[body_start] == '<')) {
          out->emplace_back(ToLower(piece.substr(0, name_end)),
                            Trim(piece.substr(body_start)));
        } else {
          out->emplace_back(std::string(), piece);
        }
      }
    }
  };
  size_t i = 0;
  while (i < n) {
    const char c = body[i];
    if (in_string) {
      if (c == '\\' && i + 1 < n) {
        i += 2;
        continue;
      }
      if (c == quote) {
        in_string = false;
      }
      ++i;
      continue;
    }
    if (c == '"' || c == '\'') {
      in_string = true;
      quote = c;
      ++i;
      continue;
    }
    if (c == '{' || c == '(' || c == '[' || c == '<') {
      ++depth;
      ++i;
      continue;
    }
    if (c == '}' || c == ')' || c == ']' || c == '>') {
      if (depth > 0) {
        --depth;
      }
      ++i;
      continue;
    }
    if (depth == 0 && (c == ',' || c == ';')) {
      flush(i);
      ++i;
      while (i < n &&
             (std::isspace(static_cast<unsigned char>(body[i])) != 0)) {
        ++i;
      }
      start = i;
      continue;
    }
    if (depth == 0 && (std::isspace(static_cast<unsigned char>(c)) != 0)) {
      // Whitespace ends the member only when an identifier-colon follows
      // ("field: value field2: value2"); bare values keep their spaces.
      size_t j = i;
      while (j < n &&
             (std::isspace(static_cast<unsigned char>(body[j])) != 0)) {
        ++j;
      }
      if (j < n && ProtoIsIdentStart(body[j])) {
        size_t k = j + 1;
        while (k < n &&
               (std::isalnum(static_cast<unsigned char>(body[k])) != 0 ||
                body[k] == '_' || body[k] == '.' || body[k] == ']')) {
          ++k;
        }
        while (k < n &&
               (std::isspace(static_cast<unsigned char>(body[k])) != 0)) {
          ++k;
        }
        if (k < n && (body[k] == ':' || body[k] == '{' || body[k] == '<') &&
            k > j) {
          flush(i);
          i = j;
          start = i;
          continue;
        }
      }
      ++i;
      continue;
    }
    ++i;
  }
  flush(n);
}

// Canonical form of one proto/struct value: nested braced bodies sort their
// members so field order never affects matching.
std::string CanonicalizeProtoValue(const std::string& value);

std::string CanonicalizeProtoBody(const std::string& body) {
  std::vector<std::pair<std::string, std::string>> members;
  SplitProtoMembers(body, &members);
  std::vector<std::string> canon;
  canon.reserve(members.size());
  for (const auto& [name, raw] : members) {
    canon.push_back(name + "=" + CanonicalizeProtoValue(raw));
  }
  std::sort(canon.begin(), canon.end());
  std::string joined;
  joined.reserve(canon.size());
  for (const std::string& item : canon) {
    if (!joined.empty()) {
      joined.push_back('|');
    }
    joined += item;
  }
  return joined;
}

std::string CanonicalizeProtoValue(const std::string& value) {
  const std::string trimmed = Trim(value);
  if (trimmed.size() >= 2 && trimmed.front() == '{' && trimmed.back() == '}') {
    return "{" + CanonicalizeProtoBody(trimmed.substr(1, trimmed.size() - 2)) +
           "}";
  }
  if (trimmed.size() >= 2 && trimmed.front() == '<' && trimmed.back() == '>') {
    return "{" + CanonicalizeProtoBody(trimmed.substr(1, trimmed.size() - 2)) +
           "}";
  }
  if (trimmed.size() >= 2 &&
      ((trimmed.front() == '\'' && trimmed.back() == '\'') ||
       (trimmed.front() == '"' && trimmed.back() == '"'))) {
    // Quoted payloads are case-sensitive string data: keep the inner text
    // verbatim; only bare tokens (enum/bool/field-name side) casefold.
    return "\"" + trimmed.substr(1, trimmed.size() - 2) + "\"";
  }
  return ToLower(NormalizeWsText(trimmed));
}

// Order-insensitive whole-proto comparison: both bodies must parse into
// fully-named member multisets (positional struct tokens are rejected so
// order-sensitive flows keep using the positional paths).
bool ProtoBodiesMatch(const std::string& want_body,
                      const std::string& actual_body) {
  std::vector<std::pair<std::string, std::string>> want_members;
  std::vector<std::pair<std::string, std::string>> actual_members;
  SplitProtoMembers(want_body, &want_members);
  SplitProtoMembers(actual_body, &actual_members);
  if (want_members.empty() || want_members.size() != actual_members.size()) {
    return false;
  }
  bool all_named = true;
  for (const auto& [name, raw] : want_members) {
    if (name.empty()) {
      all_named = false;
      break;
    }
  }
  for (const auto& [name, raw] : actual_members) {
    if (name.empty()) {
      all_named = false;
      break;
    }
  }
  if (!all_named) {
    return false;
  }
  return CanonicalizeProtoBody(want_body) == CanonicalizeProtoBody(actual_body);
}

// Decode a scalar extracted from an encoded STRUCT/PROTO JSON object before
// comparing it with a typed golden.  Keeping everything as VARCHAR here
// makes JSON-encoded BOOL/number members fail against `false`/numeric tokens.
Value ParseEmbeddedComplianceValue(std::string token) {
  token = Trim(token);
  if (ToLower(token) == "null") {
    return {};
  }
  if (ToLower(token) == "true") {
    return Value(int64_t{1});
  }
  if (ToLower(token) == "false") {
    return Value(int64_t{0});
  }
  if (token.size() >= 2 && token.front() == '"' && token.back() == '"') {
    return Value(Unquote(token));
  }
  size_t consumed = 0;
  try {
    const int64_t value = std::stoll(token, &consumed);
    if (consumed == token.size()) {
      return Value(value);
    }
  } catch (...) {
    // Not an integer (invalid or out of range): fall through to the double /
    // VARCHAR fallbacks below.
    consumed = 0;
  }
  try {
    const double value = std::stod(token, &consumed);
    if (consumed == token.size()) {
      return Value(value);
    }
  } catch (...) {
    // Not a number either: keep the raw token as a VARCHAR value.
    consumed = 0;
  }
  return Value(std::move(token));
}

}  // namespace

bool ComplianceErrorTextMatches(std::string_view expected,
                                std::string_view actual,
                                std::string_view observed_code) {
  // Folds case and every non-alphanumeric run to single spaces so
  // punctuation, quoting style and "[at L:C]" position suffixes never
  // decide the match.
  auto normalize = [](std::string_view text) {
    std::string out;
    out.reserve(text.size());
    bool pending_space = false;
    for (const char ch : text) {
      const auto u = static_cast<unsigned char>(ch);
      if (std::isalnum(u) != 0) {
        if (pending_space && !out.empty()) {
          out.push_back(' ');
        }
        pending_space = false;
        out.push_back(static_cast<char>(std::tolower(u)));
      } else {
        pending_space = true;
      }
    }
    return out;
  };
  // Sorted-unique alphanumeric tokens of length >= 4: the content words two
  // differently-worded messages can still agree on.
  auto significant = [](const std::string& text) {
    std::vector<std::string> tokens;
    tokens.reserve(text.size());
    size_t i = 0;
    while (i < text.size()) {
      while (i < text.size() && text[i] == ' ') {
        ++i;
      }
      size_t j = i;
      while (j < text.size() && text[j] != ' ') {
        ++j;
      }
      if (j - i >= 4) {
        tokens.push_back(text.substr(i, j - i));
      }
      i = j;
    }
    std::sort(tokens.begin(), tokens.end());
    tokens.erase(std::unique(tokens.begin(), tokens.end()), tokens.end());
    return tokens;
  };
  auto shared_count = [](const std::vector<std::string>& a,
                         const std::vector<std::string>& b) {
    size_t shared = 0;
    size_t i = 0;
    size_t j = 0;
    while (i < a.size() && j < b.size()) {
      if (a[i] == b[j]) {
        ++shared;
        ++i;
        ++j;
      } else if (a[i] < b[j]) {
        ++i;
      } else {
        ++j;
      }
    }
    return shared;
  };
  // StatusCode names as spelled by common/constants.hpp ToString; used to
  // recover the engine's error category from "<Name>: message" reports.
  auto is_status_code_name = [](std::string_view name) {
    static constexpr std::string_view kNames[] = {
        // NOLINT(modernize-avoid-c-arrays)
        // NOLINT(modernize-avoid-c-arrays)
        "Unknown",
        "Success",
        "NoSpace",
        "Conflicts",
        "Duplicates",
        "UnknownType",
        "NotExists",
        "NotImplemented",
        "TooBigData",
        "AmbiguousQuery",
        "IsInfinity",
        "Deleted",
        "Corrupt",
        "IOError",
        "InvalidArgument",
        "RuntimeError",
        "MemoryLimitExceeded",
    };
    for (const std::string_view n :  // NOLINT(readability-use-anyofallof)
         kNames) {                   // NOLINT(readability-use-anyofallof)
      if (n == name) {
        return true;
      }
    }
    return false;
  };
  // GoogleSQL generic::<code> is the stable semantic signal; tinylamb renders
  // the same failure classes under its own StatusCode vocabulary. Each entry
  // lists the StatusCode names accepted for that absl code.
  auto code_allowed = [](std::string_view generic, std::string_view observed) {
    std::string_view allowed;
    if (generic == "invalid_argument") {
      allowed =
          "InvalidArgument|Duplicates|NotExists|AmbiguousQuery|UnknownType|"
          "NotImplemented";
    } else if (generic == "out_of_range") {
      allowed =
          "IsInfinity|InvalidArgument|TooBigData|RuntimeError|"
          "UnknownType";
    } else if (generic == "unimplemented") {
      allowed = "NotImplemented|InvalidArgument|Unknown";
    } else if (generic == "not_found") {
      allowed = "NotExists|InvalidArgument";
    } else if (generic == "already_exists") {
      allowed = "Duplicates|InvalidArgument";
    } else if (generic == "failed_precondition") {
      allowed = "InvalidArgument|RuntimeError|Duplicates|Conflicts";
    } else if (generic == "internal") {
      allowed = "Unknown|RuntimeError|Corrupt";
    } else if (generic == "resource_exhausted") {
      allowed = "MemoryLimitExceeded|NoSpace|TooBigData|RuntimeError";
    } else if (generic == "aborted") {
      allowed = "Conflicts|RuntimeError";
    } else if (generic == "data_loss") {
      allowed = "Corrupt";
    } else {
      return false;
    }
    size_t start = 0;
    while (start <= allowed.size()) {
      const size_t bar = allowed.find('|', start);
      const std::string_view tok = allowed.substr(
          start, bar == std::string_view::npos ? bar : bar - start);
      if (tok == observed) {
        return true;
      }
      if (bar == std::string_view::npos) {
        break;
      }
      start = bar + 1;
    }
    return false;
  };
  std::string_view msg = expected;
  std::string_view generic_code;
  // GoogleSQL goldens spell errors as "generic::<code>: <message> [at L:C]";
  // keep the code for category matching and only the message for text.
  if (msg.starts_with("generic::")) {
    const size_t colon = msg.find(':', std::string_view("generic::").size());
    if (colon != std::string_view::npos) {
      generic_code = msg.substr(std::string_view("generic::").size(),
                                colon - std::string_view("generic::").size());
      msg = msg.substr(colon + 1);
    }
  }
  const size_t at_pos = msg.find("[at ");
  if (at_pos != std::string_view::npos) {
    msg = msg.substr(0, at_pos);
  }
  const std::string want = normalize(msg);
  const std::string got = normalize(actual);
  // The engine's error category: a "<Name>:" prefix inside the reported text
  // wins (executor errors embed ToString(status)); otherwise fall back to
  // the caller-supplied Status code name (prepare errors carry no prefix).
  std::string_view observed = observed_code;
  {
    const size_t colon = actual.find(':');
    if (colon != std::string_view::npos &&
        is_status_code_name(actual.substr(0, colon))) {
      observed = actual.substr(0, colon);
    }
  }
  if (want.empty() || got.empty()) {
    // No message to compare on one side.  Only a matching error category can
    // vouch: an empty expectation ("ERROR:" or "ERROR: generic::code:" with
    // no text) must never become a wildcard that passes any error.
    return !generic_code.empty() && code_allowed(generic_code, observed);
  }
  if (got.find(want) != std::string::npos) {
    return true;
  }
  // Reverse containment covers goldens more verbose than the engine's own
  // wording ("... in MOD", argument positions); require the observed message
  // to be comparably sized so a terse unrelated error cannot pass.
  if (got.size() * 2 >= want.size() && want.find(got) != std::string::npos) {
    return true;
  }
  // GoogleSQL and tinylamb word the same failures differently; accept when
  // the messages still share content words — one word suffices alongside a
  // matching generic code, two otherwise.
  const std::vector<std::string> want_tokens = significant(want);
  const std::vector<std::string> got_tokens = significant(got);
  const size_t shared = shared_count(want_tokens, got_tokens);
  if (!generic_code.empty() && code_allowed(generic_code, observed)) {
    return want_tokens.empty() || shared >= 1;
  }
  return shared >= 2;
}

bool ComplianceValueMatches(const Value& actual, std::string_view expected) {
  const std::string want = Trim(expected);
  if (ToLower(want) == "null") {
    return actual.IsNull();
  }
  if (want.starts_with("ARRAY<") && want.ends_with("(NULL)")) {
    return actual.IsNull();
  }
  if (actual.IsNull()) {
    return false;
  }
  if (actual.IsArray()) {
    std::string sql_type;
    std::vector<std::string> elements;
    bool unordered = false;
    if (!ParseArrayToken(want, &sql_type, &elements, &unordered)) {
      return false;
    }
    std::string lower_type = ToLower(sql_type);
    const std::string lower_actual_type = ToLower(actual.ArrayElementSqlType());
    // ENUM<T> / PROTO<T> goldens match engines that carry the bare type path
    // (ENUM<googlesql_test.X> vs "googlesql_test.X").
    if (!lower_type.empty() && lower_type != lower_actual_type &&
        (lower_type.starts_with("enum<") || lower_type.starts_with("proto<")) &&
        lower_type.back() == '>') {
      const std::string unwrapped =
          Trim(lower_type.substr(lower_type.find('<') + 1,
                                 lower_type.size() - lower_type.find('<') - 2));
      if (unwrapped == lower_actual_type) {
        lower_type = lower_actual_type;
      }
    }
    // Engines that cannot name nested message element types report the bare
    // PROTO/STRUCT tag; judge those by element values.
    const auto bare_message_tag = [](const std::string& t) {
      return t == "proto" || t == "struct";
    };
    if (bare_message_tag(lower_actual_type) &&
        (lower_type.starts_with(lower_actual_type + "<"))) {
      lower_type = lower_actual_type;
    }
    // Engines that store enums as their member-name strings report
    // STRING-typed arrays where the reference prints ENUM<T>.
    if (lower_type.starts_with("enum<") && lower_actual_type == "string") {
      lower_type = lower_actual_type;
    }
    if (!lower_type.empty() && lower_type != lower_actual_type) {
      const bool struct_or_proto =
          lower_type.starts_with("struct<") || lower_type.starts_with("proto<");
      bool encoded_message_elements = lower_actual_type == "string";
      if (!encoded_message_elements && lower_actual_type == "int64") {
        encoded_message_elements = std::ranges::all_of(
            actual.ArrayElements(), [](const Value& element) {
              if (element.IsNull() || element.type != ValueType::kVarChar) {
                return element.IsNull();
              }
              const std::string_view text = element.value.varchar_value;
              return text.find(':') != std::string_view::npos ||
                     (text.size() >= 2 && text.front() == '{' &&
                      text.back() == '}');
            });
      }
      if (!struct_or_proto || !encoded_message_elements) {
        bool all_null = true;
        for (const auto& elem : actual.ArrayElements()) {
          if (!elem.IsNull()) {
            all_null = false;
            break;
          }
        }
        // The engine stores every integer as INT64 and booleans as nonzero
        // INT64, so declared INT32/UINT64/BOOL/FLOAT element types surface as
        // INT64/DOUBLE at runtime; judge those by element values instead of
        // the type tag.
        auto is_int_family = [](const std::string& t) {
          return t == "bool" || t == "boolean" || t == "int32" ||
                 t == "sint32" || t == "uint32" || t == "int64" ||
                 t == "uint64";
        };
        auto is_float_family = [](const std::string& t) {
          return t == "float" || t == "float32" || t == "float64" ||
                 t == "double" || t == "real";
        };
        auto normalize_float = [](const std::string& t) -> std::string {
          if (t == "float64") {
            return "double";
          }
          if (t == "float32" || t == "real") {
            return "float";
          }
          return t;
        };
        const std::string norm_want = normalize_float(lower_type);
        const std::string norm_actual = normalize_float(lower_actual_type);
        const bool compatible =
            (is_int_family(norm_want) && is_int_family(norm_actual)) ||
            (is_float_family(norm_want) && is_float_family(norm_actual)) ||
            ((norm_want == "bytes") && norm_actual == "string");
        if (!all_null && !compatible) {
          return false;
        }
      }
    }
    if (elements.size() != actual.ArrayElements().size()) {
      return false;
    }
    if (unordered) {
      // The reference does not guarantee element order: match as a multiset.
      // Token compatibility is asymmetric ("true" accepts any nonzero INT64
      // while "1" accepts only INT64 1), so a greedy first-fit can consume a
      // token a later element needed; use bipartite matching (Kuhn) instead.
      const auto& elems = actual.ArrayElements();
      const size_t m = elems.size();
      std::vector<int64_t> match_actual(m, -1);
      const auto try_match = [&](auto&& self, size_t expected_idx,
                                 std::vector<char>& seen) -> bool {
        for (size_t j = 0; j < m; ++j) {
          if (seen[j] != 0 ||
              !ComplianceValueMatches(elems[j], elements[expected_idx])) {
            continue;
          }
          seen[j] = 1;
          if (match_actual[j] < 0 ||
              self(self, static_cast<size_t>(match_actual[j]), seen)) {
            match_actual[j] = static_cast<int64_t>(expected_idx);
            return true;
          }
        }
        return false;
      };
      for (size_t e = 0; e < elements.size(); ++e) {
        std::vector<char> seen(m, 0);
        if (!try_match(try_match, e, seen)) {
          return false;
        }
      }
      return true;
    }
    for (size_t i = 0; i < elements.size(); ++i) {
      if (!ComplianceValueMatches(actual.ArrayElements()[i], elements[i])) {
        return false;
      }
    }
    return true;
  }

  if (ToLower(want) == "true") {
    return actual.type == ValueType::kInt64 && actual.value.int_value != 0;
  }
  if (ToLower(want) == "false") {
    return actual.type == ValueType::kInt64 && actual.value.int_value == 0;
  }
  if (actual.type == ValueType::kInt64) {
    // A quoted ("5") or bytes (b"5") golden declares a STRING expectation.
    // Accepting its digits as an INT64 would hide a type-tag confusion — the
    // VARCHAR branch below rejects the mirror-image case for the same reason.
    if ((want.size() >= 2 && want.front() == '"' && want.back() == '"') ||
        (want.size() >= 3 && (want.front() == 'b' || want.front() == 'B') &&
         want[1] == '"' && want.back() == '"')) {
      return false;
    }
    // std::stoll parses only a numeric prefix ("2.5" -> 2, "1e3" -> 1), so
    // require the whole golden to be consumed. Otherwise a DOUBLE golden
    // would match a truncated INT64 value and hide a real rounding bug.
    size_t consumed = 0;
    try {
      const int64_t parsed = std::stoll(want, &consumed);
      if (consumed == want.size()) {
        return actual.value.int_value == parsed;
      }
    } catch (...) {
      // Overflow (a UINT64 magnitude) or a non-numeric golden: retry below.
      consumed = 0;
    }
    // Goldens may carry UINT64 magnitudes (2^63 .. 2^64-1) that engines
    // storing every integer as INT64 keep as their two's-complement bit
    // pattern; compare those on the unsigned bit representation.
    try {
      const uint64_t parsed = std::stoull(want, &consumed);
      if (consumed == want.size()) {
        return static_cast<uint64_t>(actual.value.int_value) == parsed;
      }
    } catch (...) {
      return false;
    }
    return false;
  }
  if (actual.type == ValueType::kDouble) {
    const std::string lower_want = ToLower(want);
    if (lower_want == "nan" || lower_want == "+nan" || lower_want == "-nan") {
      return std::isnan(actual.value.double_value);
    }
    if (lower_want == "inf" || lower_want == "+inf" ||
        lower_want == "infinity" || lower_want == "+infinity") {
      return std::isinf(actual.value.double_value) &&
             actual.value.double_value > 0;
    }
    if (lower_want == "-inf" || lower_want == "-infinity") {
      return std::isinf(actual.value.double_value) &&
             actual.value.double_value < 0;
    }
    char* end = nullptr;
    errno = 0;
    const double parsed = std::strtod(want.c_str(), &end);
    if (end == want.c_str()) {
      return false;
    }
    const double got = actual.value.double_value;
    if (std::isnan(got) || std::isnan(parsed)) {
      return false;
    }
    if (std::isinf(got) || std::isinf(parsed)) {
      return got == parsed;
    }
    if (parsed == 0.0) {
      return got == 0.0;
    }
    const double scale =
        std::max(  // NOLINT(modernize-min-max-use-initializer-list)
            std::max(
                std::fabs(got),
                std::fabs(
                    parsed)),  // NOLINT(modernize-min-max-use-initializer-list)
            std::numeric_limits<double>::min());
    // A few-ULP relative bound: goldens produced by the same IEEE-754
    // computation agree to the last bits, so a looser tolerance hides real
    // discrepancies in the 7th significant digit. The DBL_MIN floor keeps
    // subnormal doubles comparable at a few subnormal ULPs.
    if (std::fabs(got - parsed) <=
        8.0 * std::numeric_limits<double>::epsilon() * scale) {
      return true;
    }
    // FLOAT columns surface as DOUBLE carrying a widened float32; when the
    // observed value is exactly float32-representable the column is FLOAT
    // and the golden only needs to agree at float32 precision.
    if (static_cast<double>(static_cast<float>(got)) == got &&
        static_cast<float>(got) == static_cast<float>(parsed)) {
      return true;
    }
    return false;
  }

  if (actual.type == ValueType::kVarChar) {
    if (want.size() >= 3 && (want[0] == 'b' || want[0] == 'B') &&
        want[1] == '"' && want.back() == '"') {
      return std::string(actual.value.varchar_value) == Unquote(want.substr(1));
    }
    // A bare numeric golden (2, -3.5, 1e3) declares an INT64/DOUBLE
    // expectation; a VARCHAR actual carrying the same digits is a type-tag
    // confusion, not a match. Quoted goldens ("2") and complex shapes
    // ({...}, ARRAY<...>, proto text) stay comparable below.
    if (want.size() < 2 || want.front() != '"' || want.back() != '"') {
      size_t consumed = 0;
      try {
        std::stoll(want, &consumed);
        if (consumed == want.size()) {
          return false;
        }
      } catch (   // NOLINT(bugprone-empty-catch)
          ...) {  // NOLINT(bugprone-empty-catch) non-numeric golden is expected
      }
      try {
        std::stod(want, &consumed);
        if (consumed == want.size()) {
          return false;
        }
      } catch (   // NOLINT(bugprone-empty-catch)
          ...) {  // NOLINT(bugprone-empty-catch) non-numeric golden is expected
      }
    }
    const std::string unquoted = Unquote(want);
    const std::string actual_str = std::string(actual.value.varchar_value);
    if (actual_str == unquoted) {
      return true;
    }
    // APPROX_TOP_* returns ARRAY<STRUCT<...>>.  The compact runtime stores
    // each struct element as a positional textual value; compare its fields
    // recursively against the positional struct token in the golden.
    if (actual_str.size() >= 2 && actual_str.front() == '{' &&
        actual_str.back() == '}' && want.size() >= 2 && want.front() == '{' &&
        want.back() == '}') {
      const std::vector<std::string> actual_fields =
          SplitTopLevel(actual_str.substr(1, actual_str.size() - 2));
      const std::vector<std::string> expected_fields =
          SplitTopLevel(want.substr(1, want.size() - 2));
      if (actual_fields.size() == expected_fields.size()) {
        bool matched = true;
        for (size_t i = 0; i < actual_fields.size(); ++i) {
          if (!ComplianceValueMatches(
                  ParseEmbeddedComplianceValue(actual_fields[i]),
                  expected_fields[i])) {
            matched = false;
            break;
          }
        }
        if (matched) {
          return true;
        }
      }
    }
    // Some STRUCT constructors contain arrays whose elements are themselves
    // encoded structs.  The execution layer keeps that value as its textual
    // ARRAY<T>[...] representation, so compare the parsed elements using the
    // same recursive matcher as native arrays.
    if (want.starts_with("ARRAY<") && actual_str.starts_with("ARRAY<")) {
      std::string actual_type;
      std::vector<std::string> actual_elements;
      std::string expected_type;
      std::vector<std::string> expected_elements;
      if (ParseArrayToken(actual_str, &actual_type, &actual_elements) &&
          ParseArrayToken(want, &expected_type, &expected_elements) &&
          actual_elements.size() == expected_elements.size()) {
        bool matched = true;
        for (size_t i = 0; i < actual_elements.size(); ++i) {
          if (!ComplianceValueMatches(
                  ParseEmbeddedComplianceValue(actual_elements[i]),
                  expected_elements[i])) {
            matched = false;
            break;
          }
        }
        if (matched) {
          return true;
        }
      }
    }
    auto compact_json = [](std::string_view text) {
      std::string compact;
      compact.reserve(text.size());
      bool quoted = false;
      for (size_t i = 0; i < text.size(); ++i) {
        const char c = text[i];
        if (quoted) {
          compact.push_back(c);
          if (c == '\\' && i + 1 < text.size()) {
            compact.push_back(text[++i]);
          } else if (c == '"') {
            quoted = false;
          }
        } else if (c == '"') {
          quoted = true;
          compact.push_back(c);
        } else if (!std::isspace(static_cast<unsigned char>(c))) {
          compact.push_back(c);
        }
      }
      return compact;
    };
    if ((actual_str.starts_with("{") || actual_str.starts_with("[")) &&
        (unquoted.starts_with("{") || unquoted.starts_with("[")) &&
        compact_json(actual_str) == compact_json(unquoted)) {
      return true;
    }
    // JSON arrays inside encoded STRUCT values need the same recursive
    // comparison as native ARRAY Values.
    if (want.starts_with("ARRAY<") && actual_str.size() >= 2 &&
        actual_str.front() == '[' && actual_str.back() == ']') {
      std::string expected_type;
      std::vector<std::string> expected_elements;
      if (ParseArrayToken(want, &expected_type, &expected_elements)) {
        const std::string body = actual_str.substr(1, actual_str.size() - 2);
        const std::vector<std::string> actual_elements = SplitTopLevel(body);
        if (actual_elements.size() == expected_elements.size()) {
          bool matched = true;
          for (size_t i = 0; i < actual_elements.size(); ++i) {
            if (!ComplianceValueMatches(
                    ParseEmbeddedComplianceValue(actual_elements[i]),
                    expected_elements[i])) {
              matched = false;
              break;
            }
          }
          if (matched) {
            return true;
          }
        }
      }
    }
    // Proto text may omit the outer braces that STRUCT goldens use.
    if (want.size() >= 2 && want.front() == '{' && want.back() == '}' &&
        !actual_str.empty() && actual_str.front() != '{' &&
        ProtoBodiesMatch(want.substr(1, want.size() - 2), actual_str)) {
      return true;
    }
    // STRUCT/PROTO values are represented by the execution layer as JSON or
    // proto-text strings.  Compliance goldens use positional STRUCT syntax,
    // so compare JSON object members in their declared order when the field
    // names themselves are only an encoding detail.
    if (want.size() >= 2 && want.front() == '{' && want.back() == '}' &&
        actual_str.size() >= 2 && actual_str.front() == '{' &&
        actual_str.back() == '}') {
      auto split_object_values = [](std::string_view body) {
        std::vector<std::string> values;
        values.reserve(body.size());
        for (const std::string& member : SplitTopLevel(body)) {
          int depth = 0;
          bool quoted = false;
          char quote = '\0';
          for (size_t i = 0; i < member.size(); ++i) {
            const char c = member[i];
            if (quoted) {
              if (c == '\\' && i + 1 < member.size()) {
                ++i;
              } else if (c == quote) {
                quoted = false;
              }
              continue;
            }
            if (c == '"' || c == '\'') {
              quoted = true;
              quote = c;
            } else if (c == '{' || c == '[' || c == '(') {
              ++depth;
            } else if (c == '}' || c == ']' || c == ')') {
              --depth;
            } else if (c == ':' && depth == 0) {
              std::string value = Trim(member.substr(i + 1));
              values.push_back(std::move(value));
              break;
            }
          }
        }
        return values;
      };
      const std::vector<std::string> actual_values = split_object_values(
          std::string_view(actual_str).substr(1, actual_str.size() - 2));
      const std::vector<std::string> expected_values =
          SplitTopLevel(std::string_view(want).substr(1, want.size() - 2));
      if (!actual_values.empty() &&
          actual_values.size() == expected_values.size()) {
        bool matched = true;
        for (size_t i = 0; i < actual_values.size(); ++i) {
          if (!ComplianceValueMatches(
                  ParseEmbeddedComplianceValue(actual_values[i]),
                  expected_values[i])) {
            matched = false;
            break;
          }
        }
        if (matched) {
          return true;
        }
      }
    }
    if (actual_str == unquoted) {
      return true;
    }
    auto normalize_ws = [](std::string_view str) -> std::string {
      return NormalizeWsText(str);
    };
    auto strip_order_annotations = [](const std::string& s) {
      std::string out = s;
      for (const char* note : {"known order:", "unknown order:"}) {
        size_t pos = 0;
        const size_t note_len = std::char_traits<char>::length(note);
        while ((pos = ToLower(out).find(note)) != std::string::npos) {
          out.erase(pos, note_len);
        }
      }
      return out;
    };
    if (normalize_ws(strip_order_annotations(actual_str)) ==
            normalize_ws(strip_order_annotations(unquoted)) ||
        normalize_ws(strip_order_annotations(actual_str)) ==
            normalize_ws(strip_order_annotations(want))) {
      return true;
    }
    // The plain whitespace-normalized comparison is a catch-all that would
    // accept any two strings differing only in spacing. Restrict it to
    // proto-text-shaped goldens (brace-wrapped payloads), where formatting is
    // genuinely insignificant.
    const bool proto_shaped = want.find('{') != std::string::npos ||
                              unquoted.find('{') != std::string::npos ||
                              actual_str.find('{') != std::string::npos;
    if (proto_shaped &&
        (normalize_ws(actual_str) == normalize_ws(unquoted) ||
         normalize_ws(actual_str) == normalize_ws(want) ||
         normalize_ws("{" + actual_str + "}") == normalize_ws(want) ||
         normalize_ws("{" + actual_str + "}") == normalize_ws(unquoted))) {
      return true;
    }
    const std::string norm_actual = Unquote(actual_str);
    // Whole-proto goldens may wrap the payload in extra STRUCT brace layers
    // ("{{ field: v ... }}"): strip every layer from the golden side and
    // compare member multisets so field order never affects matching.
    {
      std::string want_body = want;
      while (want_body.size() >= 2 && want_body.front() == '{' &&
             want_body.back() == '}') {
        want_body = Trim(want_body.substr(1, want_body.size() - 2));
      }
      std::string actual_body = norm_actual.empty() ? actual_str : norm_actual;
      if (!want_body.empty() && !actual_body.empty()) {
        if (ProtoBodiesMatch(want_body, actual_body)) {
          return true;
        }
      }
    }
    if ("{" + actual_str + "}" == unquoted || "{" + actual_str + "}" == want ||
        "{" + norm_actual + "}" == unquoted ||
        "{" + norm_actual + "}" == want) {
      return true;
    }
    // Strips a leading `"key":` / bare `key:` marker, ignoring colons
    // that live inside brackets or quotes (e.g. ARRAY tokens carrying
    // ordering annotations).
    auto drop_key = [](std::string elem) -> std::string {
      int depth = 0;
      bool in_str = false;
      char quote = '\0';
      for (size_t i = 0; i < elem.size(); ++i) {
        const char c = elem[i];
        if (in_str) {
          if (c == '\\' && i + 1 < elem.size()) {
            ++i;
          } else if (c == quote) {
            in_str = false;
          }
          continue;
        }
        if (c == '"' || c == '\'') {
          in_str = true;
          quote = c;
        } else if (c == '<' || c == '[' || c == '(' || c == '{') {
          ++depth;
        } else if (c == '>' || c == ']' || c == ')' || c == '}') {
          if (depth > 0) {
            --depth;
          }
        } else if (c == ':' && depth == 0 && i + 1 < elem.size()) {
          return Trim(elem.substr(i + 1));
        }
      }
      return elem;
    };
    // Canonical "name=value" member form for the order-insensitive struct
    // multiset below: keeps the field name so swapped or renamed fields
    // cannot compare equal, while the value side keeps the drop_key
    // normalizations (including the NULL-typed array collapse).
    auto member_pair = [](std::string elem) -> std::string {
      int depth = 0;
      bool in_str = false;
      char quote = '\0';
      for (size_t i = 0; i < elem.size(); ++i) {
        const char c = elem[i];
        if (in_str) {
          if (c == '\\' && i + 1 < elem.size()) {
            ++i;
          } else if (c == quote) {
            in_str = false;
          }
          continue;
        }
        if (c == '"' || c == '\'') {
          in_str = true;
          quote = c;
        } else if (c == '<' || c == '[' || c == '(' || c == '{') {
          ++depth;
        } else if (c == '>' || c == ']' || c == ')' || c == '}') {
          if (depth > 0) {
            --depth;
          }
        } else if (c == ':' && depth == 0 && i + 1 < elem.size()) {
          const std::string name = ToLower(Trim(elem.substr(0, i)));
          std::string val = Trim(elem.substr(i + 1));
          const std::string lower_val = ToLower(val);
          if (lower_val.starts_with("array<") &&
              lower_val.ends_with("(null)")) {
            val = "NULL";
          }
          if (name.empty()) {
            return val;
          }
          return name +
                 "=" +  // NOLINT(performance-inefficient-string-concatenation)
                 val;   // NOLINT(performance-inefficient-string-concatenation)
        }
      }
      return elem;
    };
    if (!want.empty() && want.front() == '{' && want.back() == '}' &&
        !norm_actual.empty() && norm_actual.front() == '{' &&
        norm_actual.back() == '}') {
      // Order-insensitive comparison: protos and structs are field sets, so
      // members match as a multiset of canonicalized tokens.
      auto canonical_members = [&](const std::string& body) {
        std::vector<std::string> parts = SplitTopLevel(body);
        std::vector<std::string> out;
        out.reserve(parts.size());
        for (std::string& p : parts) {
          std::string elem = member_pair(Trim(p));
          const std::string lower_elem = ToLower(elem);
          if (lower_elem.starts_with("array<") &&
              lower_elem.ends_with("(null)")) {
            elem = "NULL";
          }
          out.push_back(normalize_ws(strip_order_annotations(elem)));
        }
        std::sort(out.begin(), out.end());
        return out;
      };
      if (canonical_members(want.substr(1, want.size() - 2)) ==
          canonical_members(norm_actual.substr(1, norm_actual.size() - 2))) {
        return true;
      }
      std::vector<std::string> want_parts =
          SplitTopLevel(want.substr(1, want.size() - 2));
      std::vector<std::string> got_parts =
          SplitTopLevel(norm_actual.substr(1, norm_actual.size() - 2));
      if (want_parts.size() == got_parts.size()) {
        bool matched = true;
        for (size_t idx = 0; idx < want_parts.size(); ++idx) {
          std::string want_elem = drop_key(Trim(want_parts[idx]));
          std::string got_elem = drop_key(Trim(got_parts[idx]));
          // STRUCT constructors are stored as JSON text.  Their string
          // fields therefore carry JSON quotes while the compliance golden
          // uses the scalar spelling (notably enum member names).
          if (got_elem.size() >= 2 && got_elem.front() == '"' &&
              got_elem.back() == '"') {
            got_elem = Unquote(got_elem);
          }
          // A NULL-typed array prints as ARRAY<T>(NULL); treat it as NULL.
          {
            const std::string lower_want = ToLower(want_elem);
            if (lower_want.starts_with("array<") &&
                lower_want.ends_with("(null)")) {
              want_elem = "NULL";
            }
          }
          if (ToLower(got_elem) == "null") {
            if (ToLower(want_elem) != "null") {
              matched = false;
              break;
            }
          } else if (ToLower(got_elem) == "true" ||
                     ToLower(got_elem) == "false") {
            if (ToLower(want_elem) != ToLower(got_elem)) {
              matched = false;
              break;
            }
          } else if (!ComplianceValueMatches(
                         ParseEmbeddedComplianceValue(got_elem), want_elem)) {
            matched = false;
            break;
          }
        }
        if (matched) {
          return true;
        }
      }
    }
    // Match timestamps where reference output includes timezone suffix like
    // -08, +00, etc.
    if (actual_str.size() >= 19 && actual_str[4] == '-' &&
        actual_str[7] == '-' && unquoted.size() > 3 &&
        (unquoted[unquoted.size() - 3] == '-' ||
         unquoted[unquoted.size() - 3] == '+') &&
        (std::isdigit(
             static_cast<unsigned char>(unquoted[unquoted.size() - 2])) != 0) &&
        (std::isdigit(
             static_cast<unsigned char>(unquoted[unquoted.size() - 1])) != 0)) {
      if (actual_str == unquoted.substr(0, unquoted.size() - 3)) {
        return true;
      }
    }
    if (actual_str.size() >= 19 && unquoted.size() >= 19 &&
        actual_str[4] == '-' && actual_str[7] == '-' && unquoted[4] == '-' &&
        unquoted[7] == '-') {
      auto parse_ts_utc = [](std::string_view s)
          -> std::pair<bool, std::pair<int64_t, int64_t>> {
        const std::string str(s);
        int Y = 0, M = 0, D = 0, h = 0, m = 0, sec = 0;
        if (sscanf(str.c_str(),  // NOLINT(cert-err34-c) field count validated
                   "%d-%d-%d %d:%d:%d", &Y, &M, &D, &h, &m, &sec) < 6) {
          return {false, {0, 0}};
        }
        int64_t sub_ns = 0;
        size_t dot = s.find('.');
        if (dot != std::string_view::npos) {
          size_t end_d = dot + 1;
          while (end_d < s.size() && s[end_d] >= '0' && s[end_d] <= '9') {
            ++end_d;
          }
          std::string frac_str(s.substr(dot + 1, end_d - (dot + 1)));
          while (frac_str.size() < 9) {
            frac_str.push_back('0');
          }
          if (frac_str.size() > 9) {
            frac_str = frac_str.substr(0, 9);
          }
          sub_ns = std::stoll(frac_str);
        }
        int tz_sec = 0;
        size_t tz_pos = s.find_first_of("+-", 11);
        if (tz_pos != std::string_view::npos && tz_pos + 1 < s.size()) {
          int tzh = 0, tzm = 0;
          if (s.find(':', tz_pos) != std::string_view::npos) {
            if (sscanf(str.c_str() + tz_pos + 1,  // NOLINT(cert-err34-c)
                       "%d:%d", &tzh, &tzm) < 2) {
              tzh = 0;
              tzm = 0;
            }
          } else if (sscanf(str.c_str() + tz_pos + 1,  // NOLINT(cert-err34-c)
                            "%d", &tzh) < 1) {
            tzh = 0;
          }
          tz_sec = (tzh * 3600 + tzm * 60) * (s[tz_pos] == '-' ? -1 : 1);
        }
        std::chrono::year_month_day ymd{
            std::chrono::year{Y}, std::chrono::month{static_cast<unsigned>(M)},
            std::chrono::day{static_cast<unsigned>(D)}};
        // sys_days{ymd} requires ymd.ok(): converting a calendar-invalid date
        // (month 13, day 45, ...) is UB and aborts in assertion builds.  The
        // sscanf field count alone cannot establish a valid date.
        if (!ymd.ok()) {
          return {false, {0, 0}};
        }
        int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count();
        int64_t total_secs =
            (days * 86400LL) + (h * 3600LL) + (m * 60LL) + sec - tz_sec;
        return {true, {total_secs, sub_ns}};
      };
      auto [ok1, utc1] = parse_ts_utc(actual_str);
      auto [ok2, utc2] = parse_ts_utc(unquoted);
      if (ok1 && ok2 && utc1 == utc2) {
        return true;
      }
    }
    return false;
  }
  if (actual.type == ValueType::kDate) {
    const std::string got = FormatDateDays(actual.value.int_value);
    return got == Unquote(want) || got == want;
  }
  return FormatComplianceValue(actual) == Unquote(want);
}

}  // namespace tinylamb
