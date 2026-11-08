/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_JSON_DOC_HPP
#define TINYLAMB_JSON_DOC_HPP

// Shared in-memory JSON document model for the JSON function family.  One
// parser and one path navigator back the constant-fold rewrite
// (expression/rewrite.cpp) and both function evaluators (the AST
// ground-truth evaluator in expression/function_call_expression.cpp and the
// relational evaluator in executor/detail/expression_eval.cpp), so a text
// accepted or produced by one route matches the others.

#include <cctype>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "expression/json_text.hpp"
#include "type/value.hpp"

namespace tinylamb::json_doc {

struct JVal;

struct JVal {
  enum Type : std::uint8_t { kNull, kBool, kNum, kStr, kArr, kObj } type{kNull};
  bool b{false};
  double num{0.0};
  std::string str{};
  std::string_view raw{};
  std::vector<std::shared_ptr<JVal>> arr{};
  std::vector<std::pair<std::string, std::shared_ptr<JVal>>> obj{};

  [[nodiscard]] std::string canonical() const {
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
          if (c == '"') {
            res += "\\\"";
          } else if (c == '\\') {
            res += "\\\\";
          } else {
            res += c;
          }
        }
        res += "\"";
        return res;
      }
      case kArr: {
        std::string res = "[";
        for (size_t i = 0; i < arr.size(); ++i) {
          if (i > 0) {
            res += ",";
          }
          if (arr[i]) {
            res += arr[i]->canonical();
          }
        }
        res += "]";
        return res;
      }
      case kObj: {
        std::string res = "{";
        for (size_t i = 0; i < obj.size(); ++i) {
          if (i > 0) {
            res += ",";
          }
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

inline void SkipWs(std::string_view& s) {
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t' ||
                        s.front() == '\n' || s.front() == '\r')) {
    s.remove_prefix(1);
  }
}

inline std::string ParseJsonStringToken(std::string_view& s) {
  std::string res;
  if (s.empty() || s.front() != '"') {
    return res;
  }
  s.remove_prefix(1);
  while (!s.empty()) {
    char c = s.front();
    s.remove_prefix(1);
    if (c == '"') {
      break;
    }
    if (c == '\\' && !s.empty()) {
      char esc = s.front();
      s.remove_prefix(1);
      if (esc == 'n') {
        res += '\n';
      } else if (esc == 't') {
        res += '\t';
      } else if (esc == 'r') {
        res += '\r';
      } else {
        res += esc;
      }
    } else {
      res += c;
    }
  }
  return res;
}

inline JVal ParseJsonSimple(std::string_view& s, size_t depth = 0);

inline JVal ParseJsonSimple(std::string_view& s, size_t depth) {
  SkipWs(s);
  JVal v;
  // Bound recursion: a hostile nest of '[' or '{' would otherwise blow the
  // stack (Bug 68, mirrored in expression_eval.cpp's parse_j).
  constexpr size_t kMaxJsonDepth = 512;
  if (depth > kMaxJsonDepth) {
    return v;
  }
  if (s.empty()) {
    return v;
  }
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
      const size_t before = s.size();
      v.arr.push_back(std::make_shared<JVal>(ParseJsonSimple(s, depth + 1)));
      // An unrecognized token consumes nothing (empty JVal); without a
      // progress check "[x" would loop forever and exhaust memory.
      if (s.size() >= before) {
        break;
      }
      SkipWs(s);
      if (!s.empty() && s.front() == ',') {
        s.remove_prefix(1);
        SkipWs(s);
      }
    }
    if (!s.empty() && s.front() == ']') {
      s.remove_prefix(1);
    }
    v.raw = start.substr(0, start.size() - s.size());
    return v;
  }
  if (s.front() == '{') {
    s.remove_prefix(1);
    v.type = JVal::kObj;
    SkipWs(s);
    while (!s.empty() && s.front() != '}') {
      SkipWs(s);
      if (s.front() != '"') {
        break;
      }
      std::string key = ParseJsonStringToken(s);
      SkipWs(s);
      if (!s.empty() && s.front() == ':') {
        s.remove_prefix(1);
        SkipWs(s);
      }
      JVal child = ParseJsonSimple(s, depth + 1);
      v.obj.emplace_back(std::move(key),
                         std::make_shared<JVal>(std::move(child)));
      SkipWs(s);
      if (!s.empty() && s.front() == ',') {
        s.remove_prefix(1);
        SkipWs(s);
      }
    }
    if (!s.empty() && s.front() == '}') {
      s.remove_prefix(1);
    }
    v.raw = start.substr(0, start.size() - s.size());
    return v;
  }
  // Number
  size_t len = 0;
  while (len < s.size() &&
         ((std::isdigit(static_cast<unsigned char>(s[len])) != 0) ||
          s[len] == '-' || s[len] == '+' || s[len] == '.' || s[len] == 'e' ||
          s[len] == 'E')) {
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

inline const JVal* NavigateJsonPath(const JVal& root, std::string_view path) {
  const JVal* cur = &root;
  if (path.empty() || path == "$") {
    return cur;
  }
  if (path.front() == '$') {
    path.remove_prefix(1);
  }
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
      if (cur->type != JVal::kObj) {
        return nullptr;
      }
      const JVal* found = nullptr;
      for (const auto& [k, child] : cur->obj) {
        if (k == key) {
          found = child.get();
          break;
        }
      }
      if (found == nullptr) {
        return nullptr;
      }
      cur = found;
    } else if (path.front() == '[') {
      path.remove_prefix(1);
      int idx = 0;
      while (!path.empty() &&
             (std::isdigit(static_cast<unsigned char>(path.front())) != 0)) {
        idx = (idx * 10) + (path.front() - '0');
        path.remove_prefix(1);
      }
      if (!path.empty() && path.front() == ']') {
        path.remove_prefix(1);
      }
      if (cur->type != JVal::kArr || idx < 0 ||
          static_cast<size_t>(idx) >= cur->arr.size()) {
        return nullptr;
      }
      cur = cur->arr[static_cast<size_t>(idx)].get();
      if (cur == nullptr) {
        return nullptr;
      }
    } else {
      break;
    }
  }
  return cur;
}

// BigQuery-style JSONTypeName: the JSON_TYPE spelling for each document kind.
inline std::string_view TypeName(const JVal& v) {
  switch (v.type) {
    case JVal::kObj:
      return "object";
    case JVal::kArr:
      return "array";
    case JVal::kStr:
      return "string";
    case JVal::kNum:
      return "number";
    case JVal::kBool:
      return "boolean";
    case JVal::kNull:
      break;
  }
  return "null";
}

// Mutation statuses for JSON_SET / JSON_INSERT / JSON_REPLACE / JSON_REMOVE.
// The callers translate them into engine error messages.
enum class OpStatus {
  kOk,
  kInvalidPath,  // path syntax error or segment/container type mismatch
  kOutOfBounds,  // array index beyond the current length (no extension)
  kExists,       // JSON_INSERT on an existing path
  kMissing       // JSON_REPLACE on a nonexistent path
};

// One parsed path hop: either an object member name or an array position.
struct PathSegment {
  bool is_index{false};
  std::string key{};
  int64_t index{0};
};

// Splits a `$`-style path into hops.  Accepts the same shapes the reader
// navigator walks: `.name`, `["name"]`, `[index]`, an optional leading `$`,
// and a bare first name.  Returns nullopt on syntax errors or empty names.
inline std::optional<std::vector<PathSegment>> ParsePathSegments(
    std::string_view path) {
  std::vector<PathSegment> segments;
  if (path.empty() || path == "$") {
    return segments;
  }
  if (path.front() == '$') {
    path.remove_prefix(1);
  }
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
      if (key.empty()) {
        return std::nullopt;
      }
      segments.push_back(PathSegment{.is_index = false, .key = std::move(key)});
    } else if (path.front() == '[') {
      path.remove_prefix(1);
      SkipWs(path);
      if (!path.empty() && path.front() == '"') {
        std::string key = ParseJsonStringToken(path);
        SkipWs(path);
        if (path.empty() || path.front() != ']' || key.empty()) {
          return std::nullopt;
        }
        path.remove_prefix(1);
        segments.push_back(
            PathSegment{.is_index = false, .key = std::move(key)});
      } else {
        bool digits =
            !path.empty() &&
            (std::isdigit(static_cast<unsigned char>(path.front())) != 0);
        if (!digits) {
          return std::nullopt;
        }
        int64_t idx = 0;
        while (!path.empty() &&
               (std::isdigit(static_cast<unsigned char>(path.front())) != 0)) {
          idx = idx * 10 + (path.front() - '0');
          path.remove_prefix(1);
        }
        SkipWs(path);
        if (path.empty() || path.front() != ']') {
          return std::nullopt;
        }
        path.remove_prefix(1);
        segments.push_back(PathSegment{.is_index = true, .index = idx});
      }
    } else if (segments.empty()) {
      // A leading bare name without '.' ("$.a" minus the '$' already
      // handled; "a.b" style).  Consume the name as the first hop.
      std::string key;
      while (!path.empty() && path.front() != '.' && path.front() != '[') {
        key += path.front();
        path.remove_prefix(1);
      }
      if (key.empty()) {
        return std::nullopt;
      }
      segments.push_back(PathSegment{.is_index = false, .key = std::move(key)});
    } else {
      return std::nullopt;
    }
  }
  return segments;
}

// Positions `parent`/`slot` on the final hop of `segments` for mutation:
// `slot` is the existing child (nullptr when the final hop is missing) and
// `parent` is always the container owning it.  Returns kOk with
// parent==nullptr when the path is empty (whole-document assignment).
// `create_intermediates` mirrors JSON_SET/JSON_INSERT (missing intermediate
// objects are created); JSON_REPLACE refuses instead.
inline OpStatus ResolveMutationTarget(JVal& root,
                                      const std::vector<PathSegment>& segments,
                                      bool create_intermediates, JVal** parent,
                                      JVal** slot,
                                      const PathSegment** final_segment) {
  *parent = nullptr;
  *slot = nullptr;
  *final_segment = nullptr;
  if (segments.empty()) {
    return OpStatus::kOk;
  }
  JVal* cur = &root;
  for (size_t i = 0; i < segments.size(); ++i) {
    const PathSegment& segment = segments[i];
    *final_segment = &segment;
    if (segment.is_index) {
      if (cur->type != JVal::kArr) {
        return OpStatus::kInvalidPath;
      }
      if (segment.index < 0 ||
          static_cast<size_t>(segment.index) >= cur->arr.size()) {
        return OpStatus::kOutOfBounds;
      }
      *parent = cur;
      *slot = cur->arr[static_cast<size_t>(segment.index)].get();
      if (i + 1 < segments.size()) {
        cur = *slot;
      }
      continue;
    }
    if (cur->type != JVal::kObj) {
      return OpStatus::kInvalidPath;
    }
    JVal* found = nullptr;
    for (auto& [key, child] : cur->obj) {
      if (key == segment.key) {
        found = child.get();
        break;
      }
    }
    if (found == nullptr) {
      if (i + 1 < segments.size()) {
        if (!create_intermediates) {
          return OpStatus::kMissing;
        }
        // Create the missing intermediate as an empty object and descend.
        cur->obj.emplace_back(segment.key,
                              std::make_shared<JVal>(JVal{.type = JVal::kObj}));
        cur = cur->obj.back().second.get();
        *parent = cur;
        *slot = nullptr;
        continue;
      }
      *parent = cur;
      *slot = nullptr;
      return OpStatus::kOk;
    }
    *parent = cur;
    *slot = found;
    if (i + 1 < segments.size()) {
      cur = found;
    }
  }
  return OpStatus::kOk;
}

// JSON_SET semantics: assign `replacement` at `path`, creating intermediate
// objects (and the final member) as needed.  Existing values are replaced.
inline OpStatus SetAtPath(JVal& root, const std::vector<PathSegment>& segments,
                          std::shared_ptr<JVal> replacement) {
  if (segments.empty()) {
    root = *replacement;
    return OpStatus::kOk;
  }
  JVal* parent = nullptr;
  JVal* slot = nullptr;
  const PathSegment* final_segment = nullptr;
  const OpStatus status =
      ResolveMutationTarget(root, segments, /*create_intermediates=*/true,
                            &parent, &slot, &final_segment);
  if (status != OpStatus::kOk) {
    return status;
  }
  if (final_segment->is_index) {
    parent->arr[static_cast<size_t>(final_segment->index)] =
        std::move(replacement);
    return OpStatus::kOk;
  }
  for (auto& [key, child] : parent->obj) {
    if (key == final_segment->key) {
      child = std::move(replacement);
      return OpStatus::kOk;
    }
  }
  parent->obj.emplace_back(final_segment->key, std::move(replacement));
  return OpStatus::kOk;
}

// JSON_INSERT: like SetAtPath but refuses to overwrite an existing value.
inline OpStatus InsertAtPath(JVal& root,
                             const std::vector<PathSegment>& segments,
                             std::shared_ptr<JVal> replacement) {
  if (segments.empty()) {
    return OpStatus::kExists;  // the whole document always exists
  }
  JVal* parent = nullptr;
  JVal* slot = nullptr;
  const PathSegment* final_segment = nullptr;
  const OpStatus status =
      ResolveMutationTarget(root, segments, /*create_intermediates=*/true,
                            &parent, &slot, &final_segment);
  if (status != OpStatus::kOk) {
    return status;
  }
  if (slot != nullptr) {
    return OpStatus::kExists;
  }
  if (final_segment->is_index) {
    return OpStatus::kExists;  // in-bounds array slots always exist
  }
  parent->obj.emplace_back(final_segment->key, std::move(replacement));
  return OpStatus::kOk;
}

// JSON_REPLACE: like SetAtPath but refuses to create anything.
inline OpStatus ReplaceAtPath(JVal& root,
                              const std::vector<PathSegment>& segments,
                              std::shared_ptr<JVal> replacement) {
  if (segments.empty()) {
    root = *replacement;
    return OpStatus::kOk;
  }
  JVal* parent = nullptr;
  JVal* slot = nullptr;
  const PathSegment* final_segment = nullptr;
  const OpStatus status =
      ResolveMutationTarget(root, segments, /*create_intermediates=*/false,
                            &parent, &slot, &final_segment);
  if (status != OpStatus::kOk) {
    return status;
  }
  if (slot == nullptr) {
    return OpStatus::kMissing;
  }
  if (final_segment->is_index) {
    parent->arr[static_cast<size_t>(final_segment->index)] =
        std::move(replacement);
    return OpStatus::kOk;
  }
  for (auto& [key, child] : parent->obj) {
    if (key == final_segment->key) {
      child = std::move(replacement);
      return OpStatus::kOk;
    }
  }
  return OpStatus::kMissing;
}

// JSON_REMOVE: clears the value at `path`.  Missing object members are a
// silent no-op (BigQuery semantics); array positions must be in bounds.
inline OpStatus RemoveAtPath(JVal& root,
                             const std::vector<PathSegment>& segments) {
  if (segments.empty()) {
    root = JVal{};  // removing the whole document yields JSON null
    return OpStatus::kOk;
  }
  JVal* parent = nullptr;
  JVal* slot = nullptr;
  const PathSegment* final_segment = nullptr;
  const OpStatus status =
      ResolveMutationTarget(root, segments, /*create_intermediates=*/false,
                            &parent, &slot, &final_segment);
  if (status != OpStatus::kOk) {
    return status;
  }
  if (slot == nullptr) {
    return OpStatus::kOk;
  }
  if (final_segment->is_index) {
    parent->arr.erase(parent->arr.begin() +
                      static_cast<ptrdiff_t>(final_segment->index));
    return OpStatus::kOk;
  }
  for (auto iter = parent->obj.begin(); iter != parent->obj.end(); ++iter) {
    if (iter->first == final_segment->key) {
      parent->obj.erase(iter);
      return OpStatus::kOk;
    }
  }
  return OpStatus::kOk;
}

// Parses `text` fully (no trailing garbage) into a document, or returns
// nullopt when the text is not valid JSON.  Callers turn that into either an
// error (the mutating family) or NULL (the LAX_* family).  The `raw` views
// of the result (and its children) point into `text`, which must outlive the
// document; canonical()/str/num stay self-contained.
inline std::optional<JVal> ParseDocument(std::string_view text) {
  std::string_view rest = text;
  JVal root = ParseJsonSimple(rest);
  SkipWs(rest);
  if (!rest.empty() || root.raw.empty()) {
    return std::nullopt;
  }
  return root;
}

namespace lax {
// Parses `text` as a JSON number the way strtod does, requiring the whole
// (whitespace-trimmed) string to be consumed.
inline std::optional<double> ParseNumberText(std::string_view text) {
  size_t begin = 0;
  size_t end = text.size();
  while (begin < end && (text[begin] == ' ' || text[begin] == '\t' ||
                         text[begin] == '\n' || text[begin] == '\r')) {
    ++begin;
  }
  while (end > begin && (text[end - 1] == ' ' || text[end - 1] == '\t' ||
                         text[end - 1] == '\n' || text[end - 1] == '\r')) {
    --end;
  }
  const std::string_view trimmed = text.substr(begin, end - begin);
  if (trimmed.empty()) {
    return std::nullopt;
  }
  std::string buffer(trimmed);
  char* parse_end = nullptr;
  const double value = std::strtod(buffer.c_str(), &parse_end);
  if (parse_end == nullptr || *parse_end != '\0') {
    return std::nullopt;
  }
  return value;
}

inline std::optional<bool> ParseBoolText(std::string_view text) {
  if (text == "true" || text == "TRUE" || text == "True") {
    return true;
  }
  if (text == "false" || text == "FALSE" || text == "False") {
    return false;
  }
  return std::nullopt;
}
}  // namespace lax

// LAX_INT64: numbers truncate toward zero, numeric strings parse, booleans
// become 1/0, everything else (including unparseable input) yields NULL.
inline Value LaxToInt64(const JVal& v) {
  switch (v.type) {
    case JVal::kNum: {
      const double d = v.num;
      if (std::isnan(d)) {
        return Value();
      }
      if (d >= 9223372036854775807.0) {
        return Value(std::numeric_limits<int64_t>::max());
      }
      if (d <= -9223372036854775808.0) {
        return Value(std::numeric_limits<int64_t>::min());
      }
      return Value(static_cast<int64_t>(d));
    }
    case JVal::kStr: {
      const std::optional<double> parsed = lax::ParseNumberText(v.str);
      if (!parsed) {
        return Value();
      }
      return LaxToInt64(JVal{.type = JVal::kNum, .num = *parsed});
    }
    case JVal::kBool:
      return Value(v.b ? int64_t{1} : int64_t{0});
    default:
      return Value();
  }
}

// LAX_FLOAT64: numbers pass through, numeric strings parse, booleans become
// 1.0/0.0, everything else yields NULL.
inline Value LaxToDouble(const JVal& v) {
  switch (v.type) {
    case JVal::kNum:
      return Value(v.num);
    case JVal::kStr: {
      const std::optional<double> parsed = lax::ParseNumberText(v.str);
      if (!parsed) {
        return Value();
      }
      return Value(*parsed);
    }
    case JVal::kBool:
      return Value(v.b ? 1.0 : 0.0);
    default:
      return Value();
  }
}

// LAX_BOOL: booleans pass through, non-zero numbers are true, "true"/"false"
// strings (and numeric strings) convert, everything else yields NULL.
inline Value LaxToBool(const JVal& v) {
  switch (v.type) {
    case JVal::kBool:
      return Value(v.b ? int64_t{1} : int64_t{0});
    case JVal::kNum:
      return Value(v.num != 0.0 ? int64_t{1} : int64_t{0});
    case JVal::kStr: {
      if (const std::optional<bool> flag = lax::ParseBoolText(v.str)) {
        return Value(*flag ? int64_t{1} : int64_t{0});
      }
      const std::optional<double> parsed = lax::ParseNumberText(v.str);
      if (!parsed) {
        return Value();
      }
      return Value(*parsed != 0.0 ? int64_t{1} : int64_t{0});
    }
    default:
      return Value();
  }
}

// LAX_STRING: scalars render as their canonical JSON text, arrays/objects
// yield NULL.
inline Value LaxToString(const JVal& v) {
  switch (v.type) {
    case JVal::kStr:
      return Value(std::string(v.str));
    case JVal::kNum:
    case JVal::kBool:
    case JVal::kNull:
      return Value(std::string(v.raw));
    default:
      return Value();
  }
}

namespace family {

// Converts a SQL argument into its JSON document form: NULL becomes JSON
// null; everything else goes through the TO_JSON text rendering (VARCHAR
// arguments become JSON strings, struct payloads stay objects) and is parsed
// back.
inline JVal JValFromSqlValue(const Value& value) {
  if (value.IsNull()) {
    return JVal{};
  }
  std::string text = json_text::ToJsonText(value);
  std::string_view rest = text;
  return ParseJsonSimple(rest);
}

// Same value-to-text rule the function evaluators use for string-ish
// arguments: the VARCHAR payload verbatim, anything else via AsString.
inline std::string TextOf(const Value& value) {
  if (value.type == ValueType::kVarChar) {
    return std::string(value.value.varchar_value);
  }
  return value.AsString();
}

inline std::string Lower(std::string_view name) {
  std::string lowered(name);
  for (char& c : lowered) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return lowered;
}

// True when `name` is one of the family members this evaluator owns.  Both
// function evaluators consult it before their own dispatch so neither grows
// a second, diverging implementation.
inline bool IsJsonFamilyFunction(std::string_view func_name) {
  static const std::unordered_set<std::string_view> kNames = {
      "json_type",   "json_keys",    "json_array_length", "json_set",
      "json_insert", "json_replace", "json_remove",       "json_array",
      "json_object", "lax_int64",    "lax_float64",       "lax_bool",
      "lax_string"};
  return kNames.contains(Lower(func_name));
}

// The shared implementation of the JSON function family:
//
//   JSON_TYPE(json[, path])              -> VARCHAR   kind name
//   JSON_KEYS(json[, path])              -> ARRAY<STRING>
//   JSON_ARRAY_LENGTH(json[, path])      -> INT64
//   JSON_SET/INSERT/REPLACE(json, path, value[, ...]) -> VARCHAR
//   JSON_REMOVE(json, path[, ...])       -> VARCHAR
//   JSON_ARRAY(value[, ...])             -> VARCHAR
//   JSON_OBJECT(key, value[, ...])       -> VARCHAR
//   LAX_INT64/FLOAT64/BOOL/STRING(json)  -> scalar (NULL on failure)
//
// Errors are InvalidArgument, matching PARSE_JSON's rejection of malformed
// input; the LAX_* conversions never error, they yield SQL NULL.
inline StatusOr<Value> Evaluate(std::string_view func_name,
                                const std::vector<Value>& arguments) {
  const std::string name = Lower(func_name);
  const auto invalid_argument = [&name](const std::string& message) {
    return StatusError(StatusCode::kInvalidArgument, name + ": " + message);
  };

  // --- readers over (json, path) ---
  if (name == "json_type" || name == "json_keys" ||
      name == "json_array_length") {
    if (arguments.empty() || arguments.size() > 2) {
      return invalid_argument("requires 1 or 2 arguments");
    }
    if (arguments[0].IsNull() ||
        (arguments.size() == 2 && arguments[1].IsNull())) {
      return Value();
    }
    const std::string text = TextOf(arguments[0]);
    if (!json_text::IsValid(text)) {
      return invalid_argument("invalid JSON text");
    }
    const std::string path = arguments.size() == 2 ? TextOf(arguments[1]) : "$";
    const std::optional<JVal> root = ParseDocument(text);
    if (!root) {
      return invalid_argument("invalid JSON text");
    }
    const JVal* current = NavigateJsonPath(*root, path);
    if (current == nullptr) {
      return Value();
    }
    if (name == "json_type") {
      return Value(std::string(TypeName(*current)));
    }
    if (name == "json_array_length") {
      if (current->type != JVal::kArr) {
        return Value();
      }
      return Value(static_cast<int64_t>(current->arr.size()));
    }
    if (current->type != JVal::kObj) {
      return Value();
    }
    std::vector<Value> keys;
    keys.reserve(current->obj.size());
    for (const auto& [key, child] : current->obj) {
      keys.emplace_back(std::string(key));
    }
    return Value::Array(std::move(keys), "STRING");
  }

  // --- mutators (json, path, value[, path, value...]) ---
  if (name == "json_set" || name == "json_insert" || name == "json_replace") {
    if (arguments.size() < 3 || arguments.size() % 2 == 0) {
      return invalid_argument("requires a JSON document and path/value pairs");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string text = TextOf(arguments[0]);
    if (!json_text::IsValid(text)) {
      return invalid_argument("invalid JSON text");
    }
    std::optional<JVal> root = ParseDocument(text);
    if (!root) {
      return invalid_argument("invalid JSON text");
    }
    for (size_t i = 1; i + 1 < arguments.size(); i += 2) {
      if (arguments[i].IsNull()) {
        return Value();
      }
      const std::optional<std::vector<PathSegment>> segments =
          ParsePathSegments(TextOf(arguments[i]));
      if (!segments) {
        return invalid_argument("invalid JSON path " + TextOf(arguments[i]));
      }
      auto replacement =
          std::make_shared<JVal>(JValFromSqlValue(arguments[i + 1]));
      OpStatus status = OpStatus::kOk;
      if (name == "json_set") {
        status = SetAtPath(*root, *segments, std::move(replacement));
      } else if (name == "json_insert") {
        status = InsertAtPath(*root, *segments, std::move(replacement));
      } else {
        status = ReplaceAtPath(*root, *segments, std::move(replacement));
      }
      switch (status) {
        case OpStatus::kOk:
          break;
        case OpStatus::kInvalidPath:
          return invalid_argument("invalid JSON path " + TextOf(arguments[i]));
        case OpStatus::kOutOfBounds:
          return invalid_argument("JSON array index out of bounds");
        case OpStatus::kExists:
          return invalid_argument("JSON path already exists");
        case OpStatus::kMissing:
          return invalid_argument("JSON path does not exist");
      }
    }
    return Value(root->canonical());
  }

  if (name == "json_remove") {
    if (arguments.size() < 2) {
      return invalid_argument("requires a JSON document and at least 1 path");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string text = TextOf(arguments[0]);
    if (!json_text::IsValid(text)) {
      return invalid_argument("invalid JSON text");
    }
    std::optional<JVal> root = ParseDocument(text);
    if (!root) {
      return invalid_argument("invalid JSON text");
    }
    for (size_t i = 1; i < arguments.size(); ++i) {
      if (arguments[i].IsNull()) {
        return Value();
      }
      const std::optional<std::vector<PathSegment>> segments =
          ParsePathSegments(TextOf(arguments[i]));
      if (!segments) {
        return invalid_argument("invalid JSON path " + TextOf(arguments[i]));
      }
      const OpStatus status = RemoveAtPath(*root, *segments);
      if (status == OpStatus::kOutOfBounds) {
        return invalid_argument("JSON array index out of bounds");
      }
      if (status == OpStatus::kInvalidPath) {
        return invalid_argument("invalid JSON path " + TextOf(arguments[i]));
      }
    }
    return Value(root->canonical());
  }

  // --- constructors ---
  if (name == "json_array") {
    JVal array{.type = JVal::kArr};
    array.arr.reserve(arguments.size());
    for (const Value& argument : arguments) {
      array.arr.push_back(std::make_shared<JVal>(JValFromSqlValue(argument)));
    }
    return Value(array.canonical());
  }

  if (name == "json_object") {
    if (arguments.size() % 2 != 0) {
      return invalid_argument("requires key/value argument pairs");
    }
    JVal object{.type = JVal::kObj};
    for (size_t i = 0; i + 1 < arguments.size(); i += 2) {
      if (arguments[i].IsNull()) {
        return invalid_argument("keys must not be NULL");
      }
      if (arguments[i].type != ValueType::kVarChar) {
        return invalid_argument("keys must be STRING values");
      }
      const std::string key = TextOf(arguments[i]);
      for (const auto& [existing, child] : object.obj) {
        if (existing == key) {
          return invalid_argument("duplicate key " + key);
        }
      }
      object.obj.emplace_back(
          key, std::make_shared<JVal>(JValFromSqlValue(arguments[i + 1])));
    }
    return Value(object.canonical());
  }

  // --- LAX_* coercions ---
  if (name == "lax_int64" || name == "lax_float64" || name == "lax_bool" ||
      name == "lax_string") {
    if (arguments.size() != 1) {
      return invalid_argument("requires 1 argument");
    }
    if (arguments[0].IsNull()) {
      return Value();
    }
    const std::string text = TextOf(arguments[0]);
    const std::optional<JVal> root = ParseDocument(text);
    if (!root) {
      return Value();  // the LAX family never errors on malformed input
    }
    if (name == "lax_int64") {
      return LaxToInt64(*root);
    }
    if (name == "lax_float64") {
      return LaxToDouble(*root);
    }
    if (name == "lax_bool") {
      return LaxToBool(*root);
    }
    return LaxToString(*root);
  }

  return StatusError(StatusCode::kInvalidArgument,
                     "unsupported function " + name);
}

}  // namespace family

}  // namespace tinylamb::json_doc

#endif  // TINYLAMB_JSON_DOC_HPP
