/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/googlesql_compliance_file.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>

#include <cstdint>
#include <filesystem>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

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
  std::ranges::transform(
      text, text.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return text;
}

bool LineIsSeparator(std::string_view line, std::string_view expected) {
  return Trim(line) == expected;
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
  if (!Trim(current).empty()) { cases.push_back(std::move(current)); }
  return cases;
}

bool ConsumeOptionLine(std::string_view line, GoogleSqlComplianceCase* out,
                       std::vector<std::string>* file_default_features) {
  const std::string trimmed = Trim(line);
  if (trimmed.size() < 2 || trimmed.front() != '[' || trimmed.back() != ']') {
    return false;
  }
  const std::string body = trimmed.substr(1, trimmed.size() - 2);
  const size_t eq = body.find('=');
  const std::string key =
      ToLower(eq == std::string::npos ? body : body.substr(0, eq));
  const std::string value =
      eq == std::string::npos ? std::string() : body.substr(eq + 1);
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
      if (!feature.empty()) { out->required_features.push_back(feature); }
    }
    return true;
  }
  if (key == "default required_features" ||
      key == "default required_feature") {
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
  if (key == "parameters" || key == "parameter") {
    for (const auto& item : SplitTopLevel(value)) {
      const std::string s = Trim(item);
      // "<expr> AS <name>" / compact "<expr> <name>": the parameter name is
      // always the final token; a trailing AS keyword belongs to the
      // separator, not the expression (expressions may contain their own
      // "as" inside CAST(...)).
      const size_t space = s.rfind(' ');
      if (space == std::string::npos) { continue; }
      std::string name = Trim(s.substr(space + 1));
      std::string expr = Trim(s.substr(0, space));
      if (ToLower(expr) == "as") {
        continue;
      }
      if (expr.size() >= 3 &&
          ToLower(expr.substr(expr.size() - 2)) == "as") {
        expr = Trim(expr.substr(0, expr.size() - 2));
      }
      if (!name.empty() && !expr.empty()) {
        out->parameters.push_back({name, expr});
      }
    }
    return true;
  }
  if (key == "default_time_zone" || key == "default default_time_zone") {
    out->default_time_zone = value;
    return true;
  }
  return true;
}


bool ParseStructRow(std::string_view text, size_t* offset,
                    std::vector<std::string>* fields) {
  while (*offset < text.size() &&
         std::isspace(static_cast<unsigned char>(text[*offset])) != 0) {
    ++*offset;
  }
  if (*offset >= text.size() || text[*offset] != '{') { return false; }
  ++*offset;
  std::string token;
  int depth = 1;
  int bracket_depth = 0;
  bool in_string = false;
  char quote = '\0';
  auto flush = [&]() {
    const std::string field = Trim(token);
    if (!field.empty()) { fields->push_back(field); }
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
      if (c == quote) { in_string = false; }
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
      if (bracket_depth > 0) { --bracket_depth; }
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
  const std::string lower = ToLower(std::string(result));
  out->unknown_order = lower.find("unknown order") != std::string::npos;
  const size_t array_open = result.find('[');
  if (array_open == std::string_view::npos) { return; }
  size_t offset = array_open + 1;
  while (offset < result.size()) {
    while (offset < result.size() &&
           std::isspace(static_cast<unsigned char>(result[offset])) != 0) {
      ++offset;
    }
    if (offset >= result.size() || result[offset] == ']') { break; }
    if (result[offset] == ',') {
      ++offset;
      continue;
    }
    // Skip an ordering annotation before the first element.
    const std::string lower_rest =
        ToLower(std::string(result.substr(offset, 14)));
    if (lower_rest.starts_with("unknown order:")) {
      offset += 14;
    } else if (lower_rest.starts_with("known order:")) {
      offset += 12;
    }
    while (offset < result.size() &&
           std::isspace(static_cast<unsigned char>(result[offset])) != 0) {
      ++offset;
    }
    std::vector<std::string> fields;
    if (result[offset] == '{') {
      if (!ParseStructRow(result, &offset, &fields)) { break; }
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
        if (c == '\\' && end + 1 < result.size()) { ++end; }
        else if (c == quote) { in_string = false; }
        ++end;
        continue;
      }
      if (c == '"' || c == '\'') {
        in_string = true;
        quote = c;
        ++end;
        continue;
      }
      if (c == '<' || c == '[' || c == '(' || c == '{') { ++depth; }
      else if (c == '>' && depth > 0) { --depth; }
      else if ((c == ')' || c == '}') && depth > 0) { --depth; }
      else if ((c == ',' && depth == 0) || c == ']') { break; }
      ++end;
    }
    const std::string token = Trim(result.substr(offset, end - offset));
    offset = end;
    if (!token.empty()) { out->expected_rows.push_back({token}); }
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
  for (const auto& [name, expr] : parameters) {
    const std::string target = "@" + name;
    size_t pos = 0;
    while ((pos = sql.find(target, pos)) != std::string::npos) {
      const size_t end = pos + target.size();
      if (end < sql.size() &&
          (std::isalnum(static_cast<unsigned char>(sql[end])) ||
           sql[end] == '_')) {
        pos = end;
        continue;
      }
      sql.replace(pos, target.size(), "(" + expr + ")");
      pos += expr.size() + 2;
    }
  }
  return sql;
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
        if (c == '\\' && i + 1 < text.size()) { ++i; } else if (c == quote) {
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
    if (!std::getline(*stream, next)) { break; }
    line.push_back(' ');
    line += next;
  }
  return line;
}

GoogleSqlComplianceCase ParseSegment(std::string_view file,
                                     std::string_view segment,
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
      if (trimmed.empty() || trimmed.starts_with('#')) { continue; }
      if (trimmed.front() == '[') {
        line = ReadLogicalOptionLine(&stream, line);
      }
      if (ConsumeOptionLine(line, &test_case, default_features)) {
        if (!test_case.default_time_zone.empty()) {
          *current_default_tz = test_case.default_time_zone;
        }
        continue;
      }
      in_options = false;
    }
    if (LineIsSeparator(line, "--")) {
      std::ostringstream rest;
      rest << stream.rdbuf();
      ParseResult(rest.str(), &test_case);
      test_case.sql = NormalizeCommentEscapes(ApplyParameters(Trim(sql), test_case.parameters));
      if (test_case.name.empty()) {
        test_case.name = test_case.prepare_database ? "prepare_database"
                                                    : "unnamed";
      }
      return test_case;
    }
    if (sql.empty()) {
      if (trimmed.empty() || trimmed.starts_with('#')) { continue; }
      if (trimmed.front() == '[') {
        line = ReadLogicalOptionLine(&stream, line);
      }
      if (ConsumeOptionLine(line, &test_case, default_features)) {
        if (!test_case.default_time_zone.empty()) {
          *current_default_tz = test_case.default_time_zone;
        }
        continue;
      }
    }
    sql += line;
    sql.push_back('\n');
  }
  test_case.sql = NormalizeCommentEscapes(ApplyParameters(Trim(sql), test_case.parameters));
  if (test_case.name.empty()) {
    test_case.name =
        test_case.prepare_database ? "prepare_database" : "unnamed";
  }
  return test_case;
}



std::string Unquote(std::string_view token) {
  if (token.size() >= 1 && (token.front() == 'b' || token.front() == 'B')) {
    token.remove_prefix(1);
  }
  if (token.size() >= 2 &&
      ((token.front() == '"' && token.back() == '"') ||
       (token.front() == '\'' && token.back() == '\''))) {

    std::string out;
    for (size_t i = 1; i + 1 < token.size(); ++i) {
      if (token[i] == '\\' && i + 2 < token.size()) {
        char next = token[++i];
        if (next == 'x' && i + 2 < token.size()) {
          std::string hex_str = std::string(token.substr(i + 1, 2));
          try {
            int val = std::stoi(hex_str, nullptr, 16);
            out.push_back(static_cast<char>(val));
            i += 2;
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
        } else if (next == '\\' || next == '\'' || next == '"') {
          out.push_back(next);
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
  std::string current;
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
      if (c == quote) { in_string = false; }
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
      if (depth > 0) { --depth; }
      current.push_back(c);
      continue;
    }
    if (c == ',' && depth == 0) {
      const std::string part = Trim(current);
      if (!part.empty()) { parts.push_back(part); }
      current.clear();
      continue;
    }
    current.push_back(c);
  }
  const std::string part = Trim(current);
  if (!part.empty()) { parts.push_back(part); }
  return parts;
}

bool ParseArrayToken(std::string_view token, std::string* sql_type,
                     std::vector<std::string>* elements,
                     bool* unordered = nullptr) {
  const std::string text = Trim(token);
  constexpr std::string_view kPrefix = "ARRAY<";
  if (text.size() < kPrefix.size() ||
      text.compare(0, kPrefix.size(), kPrefix) != 0) {
    return false;
  }
  int depth = 1;
  size_t close = std::string::npos;
  for (size_t i = kPrefix.size(); i < text.size(); ++i) {
    if (text[i] == '<') { ++depth; }
    else if (text[i] == '>') {
      --depth;
      if (depth == 0) {
        close = i;
        break;
      }
    }
  }
  if (close == std::string::npos) { return false; }
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
    if (unordered != nullptr) { *unordered = true; }
  }
  *elements = SplitTopLevel(inner);
  return true;
}

std::string QuoteComplianceString(std::string_view text, bool bytes) {
  std::string out = bytes ? "b\"" : "\"";
  for (const char c : text) {
    if (c == '"' || c == '\\') { out.push_back('\\'); }
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
    if (Trim(segment).empty()) { continue; }
    GoogleSqlComplianceCase test_case =
        ParseSegment(file, segment, &current_default_tz, &default_features);
    if (test_case.name == "unnamed" ||
        test_case.name.starts_with("prepare_database")) {
      if (test_case.name.find('_') == std::string::npos ||
          test_case.name == "unnamed" || test_case.name == "prepare_database") {
        test_case.name += "_" + std::to_string(unnamed++);
      }
    }
    if (test_case.sql.empty() && !test_case.prepare_database) { continue; }
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
    if (!entry.is_regular_file()) { continue; }
    if (entry.path().extension() != ".test") { continue; }
    files.push_back(entry.path().filename().string());
  }
  std::ranges::sort(files);
  return files;
}

bool IsDifferentialPrivacyCase(const GoogleSqlComplianceCase& test_case) {
  const std::string hay =
      ToLower(test_case.file + " " + test_case.name + " " + test_case.sql);
  if (hay.find("differential_privacy") != std::string::npos) { return true; }
  if (hay.find("differential privacy") != std::string::npos) { return true; }
  if (hay.find("anonymization") != std::string::npos) { return true; }
  if (hay.find("anon_") != std::string::npos) { return true; }
  for (const std::string& feature : test_case.required_features) {
    const std::string lower = ToLower(feature);
    if (lower.find("anonym") != std::string::npos ||
        lower.find("differential_privacy") != std::string::npos ||
        lower.find("privacy") != std::string::npos) {
      return true;
    }
  }
  return false;
}

std::string FormatComplianceValue(const Value& value) {
  if (value.IsNull()) { return "NULL"; }
  switch (value.type) {
    case ValueType::kInt64:
      return std::to_string(value.value.int_value);
    case ValueType::kDouble: {
      if (std::isnan(value.value.double_value)) { return "nan"; }
      if (std::isinf(value.value.double_value)) {
        return value.value.double_value > 0 ? "inf" : "-inf";
      }
      char buffer[64];
      auto [ptr, ec] = std::to_chars(buffer, buffer + sizeof(buffer),
                                     value.value.double_value);
      return std::string(buffer, ptr - buffer);
    }
    case ValueType::kVarChar:
      return std::string(value.value.varchar_value);
    case ValueType::kDate:
      return FormatDateDays(value.value.int_value);
    case ValueType::kArray: {
      const std::string sql_type = value.ArrayElementSqlType();
      const auto& elements = value.ArrayElements();
      std::string inner;
      if (elements.size() >= 2) { inner = "known order:"; }
      for (size_t i = 0; i < elements.size(); ++i) {
        if (i != 0) { inner += ", "; }
        const Value& element = elements[i];
        if (element.IsNull()) {
          inner += "NULL";
        } else if (sql_type == "BOOL" || sql_type == "BOOLEAN") {
          inner += element.type == ValueType::kInt64 &&
                           element.value.int_value != 0
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

bool ComplianceValueMatches(const Value& actual, std::string_view expected) {
  const std::string want = Trim(expected);
  if (ToLower(want) == "null") { return actual.IsNull(); }
  if (want.starts_with("ARRAY<") && want.ends_with("(NULL)")) {
    return actual.IsNull();
  }
  if (actual.IsNull()) { return false; }
  if (actual.IsArray()) {
    std::string sql_type;
    std::vector<std::string> elements;
    bool unordered = false;
    if (!ParseArrayToken(want, &sql_type, &elements, &unordered)) {
      return false;
    }
    std::string lower_type = ToLower(sql_type);
    const std::string lower_actual_type =
        ToLower(actual.ArrayElementSqlType());
    // ENUM<T> / PROTO<T> goldens match engines that carry the bare type path
    // (ENUM<googlesql_test.X> vs "googlesql_test.X").
    if (lower_type != lower_actual_type &&
        (lower_type.starts_with("enum<") || lower_type.starts_with("proto<")) &&
        lower_type.back() == '>') {
      const std::string unwrapped =
          Trim(lower_type.substr(lower_type.find('<') + 1,
                                 lower_type.size() - lower_type.find('<') - 2));
      if (unwrapped == lower_actual_type) { lower_type = lower_actual_type; }
    }
    // Engines that store enums as their member-name strings report
    // STRING-typed arrays where the reference prints ENUM<T>.
    if (lower_type.starts_with("enum<") && lower_actual_type == "string") {
      lower_type = lower_actual_type;
    }
    if (!lower_type.empty() && lower_type != lower_actual_type) {
      const bool struct_or_proto =
          lower_type.starts_with("struct<") || lower_type.starts_with("proto<");
      if (!struct_or_proto || lower_actual_type != "string") {
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
          if (t == "float64") { return "double"; }
          if (t == "float32" || t == "real") { return "float"; }
          return t;
        };
        const std::string norm_want = normalize_float(lower_type);
        const std::string norm_actual = normalize_float(lower_actual_type);
        const bool compatible =
            (is_int_family(norm_want) && is_int_family(norm_actual)) ||
            (is_float_family(norm_want) && is_float_family(norm_actual)) ||
            ((norm_want == "bytes") && norm_actual == "string") ||
            // Struct/proto/enum element goldens compare by member values;
            // engines that lose the declared element tag still match when
            // every element is value-compared below.
            (norm_want.starts_with("struct<") &&
             (norm_actual == "int64" || norm_actual == "string" ||
              norm_actual == "struct")) ||
            ((norm_want.starts_with("proto<") ||
              norm_want.starts_with("enum<")) &&
             norm_actual == "int64");
        if (!all_null && !compatible) {
          return false;
        }
      }
    }
    if (elements.size() != actual.ArrayElements().size()) { return false; }
    if (unordered) {
      // The reference does not guarantee element order: match as a multiset.
      std::vector<bool> used(elements.size(), false);
      for (const Value& element : actual.ArrayElements()) {
        bool found = false;
        for (size_t i = 0; i < elements.size(); ++i) {
          if (used[i]) { continue; }
          if (ComplianceValueMatches(element, elements[i])) {
            used[i] = true;
            found = true;
            break;
          }
        }
        if (!found) { return false; }
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
    try {
      return actual.value.int_value == static_cast<int64_t>(std::stoll(want));
    } catch (...) { return false; }
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
    if (end == want.c_str()) { return false; }
    const double got = actual.value.double_value;
    if (std::isnan(got) || std::isnan(parsed)) { return false; }
    if (std::isinf(got) || std::isinf(parsed)) { return got == parsed; }
    if (std::fabs(parsed) < 1e-300) {
      return std::fabs(got - parsed) <= 1e-5 * std::fabs(parsed);
    }
    return std::fabs(got - parsed) <=
           1e-6 * std::max(1.0, std::fabs(parsed));
  }

  if (actual.type == ValueType::kVarChar) {
    if (want.size() >= 3 && (want[0] == 'b' || want[0] == 'B') &&
        want[1] == '"' && want.back() == '"') {
      return std::string(actual.value.varchar_value) ==
             Unquote(want.substr(1));
    }
    const std::string unquoted = Unquote(want);
    const std::string actual_str = std::string(actual.value.varchar_value);
    if (actual_str == unquoted) { return true; }
    auto normalize_ws = [](std::string_view str) -> std::string {
      std::string out;
      bool in_space = false;
      for (char c : str) {
        if (std::isspace(static_cast<unsigned char>(c))) {
          if (!in_space && !out.empty() && out.back() != '[' && out.back() != '(' && out.back() != '{' && out.back() != ',') {
            out.push_back(' ');
          }
          in_space = true;
        } else {
          if (in_space && !out.empty() && (c == ']' || c == ')' || c == '}' || c == ',')) {
            if (out.back() == ' ') { out.pop_back(); }
          }
          in_space = false;
          out.push_back(c);
        }
      }
      if (!out.empty() && out.back() == ' ') { out.pop_back(); }
      return out;
    };
    auto strip_order_annotations = [](const std::string& s) {
      std::string out = s;
      for (const char* note : {"known order:", "unknown order:"}) {
        size_t pos;
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
    if (normalize_ws(actual_str) == normalize_ws(unquoted) ||
        normalize_ws(actual_str) == normalize_ws(want) ||
        normalize_ws("{" + actual_str + "}") == normalize_ws(want) ||
        normalize_ws("{" + actual_str + "}") == normalize_ws(unquoted)) {
      return true;
    }
    const std::string norm_actual = Unquote(actual_str);
    if ("{" + actual_str + "}" == unquoted || "{" + actual_str + "}" == want ||
        "{" + norm_actual + "}" == unquoted || "{" + norm_actual + "}" == want) { return true; }
    if (!want.empty() && want.front() == '{' && want.back() == '}' &&
        !norm_actual.empty() && norm_actual.front() == '{' && norm_actual.back() == '}') {
      std::vector<std::string> want_parts = SplitTopLevel(want.substr(1, want.size() - 2));
      std::vector<std::string> got_parts = SplitTopLevel(norm_actual.substr(1, norm_actual.size() - 2));
      if (want_parts.size() == got_parts.size()) {
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
              if (c == '\\' && i + 1 < elem.size()) { ++i; }
              else if (c == quote) { in_str = false; }
              continue;
            }
            if (c == '"' || c == '\'') { in_str = true; quote = c; }
            else if (c == '<' || c == '[' || c == '(' || c == '{') { ++depth; }
            else if (c == '>' || c == ']' || c == ')' || c == '}') {
              if (depth > 0) { --depth; }
            } else if (c == ':' && depth == 0 && i + 1 < elem.size()) {
              return Trim(elem.substr(i + 1));
            }
          }
          return elem;
        };
        bool matched = true;
        for (size_t idx = 0; idx < want_parts.size(); ++idx) {
          std::string want_elem = drop_key(Trim(want_parts[idx]));
          std::string got_elem = drop_key(Trim(got_parts[idx]));
          // A NULL-typed array prints as ARRAY<T>(NULL); treat it as NULL.
          {
            const std::string lower_want = ToLower(want_elem);
            if (lower_want.starts_with("array<") &&
                lower_want.ends_with("(null)")) {
              want_elem = "NULL";
            }
          }
          if (ToLower(got_elem) == "null") {
            if (ToLower(want_elem) != "null") { matched = false; break; }
          } else if (ToLower(got_elem) == "true" || ToLower(got_elem) == "false") {
            if (ToLower(want_elem) != ToLower(got_elem)) { matched = false; break; }
          } else if (!ComplianceValueMatches(Value(std::string(got_elem)), want_elem)) {
            matched = false; break;
          }
        }
        if (matched) { return true; }
      }
    }
    // Match timestamps where reference output includes timezone suffix like -08, +00, etc.
    if (unquoted.size() > 3 &&
        (unquoted[unquoted.size() - 3] == '-' ||
         unquoted[unquoted.size() - 3] == '+') &&
        std::isdigit(
            static_cast<unsigned char>(unquoted[unquoted.size() - 2])) &&
        std::isdigit(
            static_cast<unsigned char>(unquoted[unquoted.size() - 1]))) {
      if (actual_str == unquoted.substr(0, unquoted.size() - 3)) {
        return true;
      }
    }
    if (actual_str.size() >= 19 && unquoted.size() >= 19 &&
        actual_str[4] == '-' && actual_str[7] == '-' &&
        unquoted[4] == '-' && unquoted[7] == '-') {
      auto parse_ts_utc = [](std::string_view s) -> std::pair<bool, std::pair<int64_t, int64_t>> {
        int Y = 0, M = 0, D = 0, h = 0, m = 0, sec = 0;
        if (sscanf(s.data(), "%d-%d-%d %d:%d:%d", &Y, &M, &D, &h, &m, &sec) < 6) {
          return {false, {0, 0}};
        }
        int64_t sub_ns = 0;
        size_t dot = s.find('.');
        if (dot != std::string_view::npos) {
          size_t end_d = dot + 1;
          while (end_d < s.size() && s[end_d] >= '0' && s[end_d] <= '9') { ++end_d; }
          std::string frac_str(s.substr(dot + 1, end_d - (dot + 1)));
          while (frac_str.size() < 9) { frac_str.push_back('0'); }
          if (frac_str.size() > 9) { frac_str = frac_str.substr(0, 9); }
          sub_ns = std::stoll(frac_str);
        }
        int tz_sec = 0;
        size_t tz_pos = s.find_first_of("+-", 11);
        if (tz_pos != std::string_view::npos && tz_pos + 1 < s.size()) {
          int tzh = 0, tzm = 0;
          if (s.find(':', tz_pos) != std::string_view::npos) {
            sscanf(s.data() + tz_pos + 1, "%d:%d", &tzh, &tzm);
          } else {
            sscanf(s.data() + tz_pos + 1, "%d", &tzh);
          }
          tz_sec = (tzh * 3600 + tzm * 60) * (s[tz_pos] == '-' ? -1 : 1);
        }
        std::chrono::year_month_day ymd{std::chrono::year{Y},
                                        std::chrono::month{static_cast<unsigned>(M)},
                                        std::chrono::day{static_cast<unsigned>(D)}};
        int64_t days = std::chrono::sys_days{ymd}.time_since_epoch().count();
        int64_t total_secs = days * 86400LL + h * 3600LL + m * 60LL + sec - tz_sec;
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
