/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_JSON_TEXT_HPP
#define TINYLAMB_JSON_TEXT_HPP

// Shared strict-JSON validation for the PARSE_JSON function and the JSON
// function family's argument checks. Both function evaluators (the AST
// ground-truth evaluator in expression/ and the relational evaluator in
// executor/detail/) include this header so a text accepted by one route is
// accepted by the other.

#include <array>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

#include "common/json_string.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::json_text {

// Recursive-descent validator over RFC 8259 JSON. Trailing whitespace is
// allowed; trailing garbage is not. Nesting depth is bounded: the validator
// runs on untrusted SQL argument text, and an unbounded `[[[[...` descent
// would exhaust the stack (a crash) where a bounded one just reports invalid.
class Validator {
 public:
  explicit Validator(std::string_view text) : text_(text) {}

  [[nodiscard]] bool Valid() {
    SkipSpace();
    if (!Value()) {
      return false;
    }
    SkipSpace();
    return pos_ == text_.size();
  }

 private:
  // Deep enough for any real document; shallow enough that the per-level
  // frame (Value -> Object/Array -> Value) cannot exhaust an 8 MiB stack.
  static constexpr int kMaxDepth = 256;

  [[nodiscard]] bool Value() {
    if (depth_ >= kMaxDepth) {
      return false;
    }
    ++depth_;
    const bool ok = ValueAtDepth();
    --depth_;
    return ok;
  }

  [[nodiscard]] bool ValueAtDepth() {
    if (pos_ >= text_.size()) {
      return false;
    }
    switch (text_[pos_]) {
      case '{':
        return Object();
      case '[':
        return Array();
      case '"':
        return String();
      case 't':
        return Literal("true");
      case 'f':
        return Literal("false");
      case 'n':
        return Literal("null");
      default:
        return Number();
    }
  }

  [[nodiscard]] bool Literal(std::string_view word) {
    if (text_.substr(pos_, word.size()) != word) {
      return false;
    }
    pos_ += word.size();
    return true;
  }

  // -1 when `hex` is not a hexadecimal digit.
  [[nodiscard]] static int HexDigit(char hex) {
    if (hex >= '0' && hex <= '9') {
      return hex - '0';
    }
    if (hex >= 'a' && hex <= 'f') {
      return hex - 'a' + 10;
    }
    if (hex >= 'A' && hex <= 'F') {
      return hex - 'A' + 10;
    }
    return -1;
  }

  [[nodiscard]] bool Object() {
    ++pos_;  // '{'
    SkipSpace();
    if (pos_ < text_.size() && text_[pos_] == '}') {
      ++pos_;
      return true;
    }
    while (pos_ < text_.size()) {
      SkipSpace();
      if (pos_ >= text_.size() || text_[pos_] != '"') {
        return false;
      }
      if (!String()) {
        return false;
      }
      SkipSpace();
      if (pos_ >= text_.size() || text_[pos_] != ':') {
        return false;
      }
      ++pos_;
      SkipSpace();
      if (!Value()) {
        return false;
      }
      SkipSpace();
      if (pos_ >= text_.size()) {
        return false;
      }
      if (text_[pos_] == '}') {
        ++pos_;
        return true;
      }
      if (text_[pos_] != ',') {
        return false;
      }
      ++pos_;
    }
    return false;
  }

  [[nodiscard]] bool Array() {
    ++pos_;  // '['
    SkipSpace();
    if (pos_ < text_.size() && text_[pos_] == ']') {
      ++pos_;
      return true;
    }
    while (pos_ < text_.size()) {
      SkipSpace();
      if (!Value()) {
        return false;
      }
      SkipSpace();
      if (pos_ >= text_.size()) {
        return false;
      }
      if (text_[pos_] == ']') {
        ++pos_;
        return true;
      }
      if (text_[pos_] != ',') {
        return false;
      }
      ++pos_;
    }
    return false;
  }

  [[nodiscard]] bool String() {
    ++pos_;  // opening quote
    while (pos_ < text_.size()) {
      const char c = text_[pos_];
      if (c == '"') {
        ++pos_;
        return true;
      }
      if (static_cast<unsigned char>(c) < 0x20) {
        return false;
      }
      if (c == '\\') {
        ++pos_;
        if (pos_ >= text_.size()) {
          return false;
        }
        switch (text_[pos_]) {
          case '"':
          case '\\':
          case '/':
          case 'b':
          case 'f':
          case 'n':
          case 'r':
          case 't':
            ++pos_;
            continue;
          case 'u': {
            if (pos_ + 4 >= text_.size()) {
              return false;
            }
            uint32_t code = 0;
            for (size_t i = 1; i <= 4; ++i) {
              const int digit = HexDigit(text_[pos_ + i]);
              if (digit < 0) {
                return false;
              }
              code = (code << 4) | static_cast<uint32_t>(digit);
            }
            pos_ += 5;
            // Surrogate pairs: a high surrogate must be followed by a low
            // one; a lone low surrogate is invalid.
            if (code >= 0xD800 && code <= 0xDBFF) {
              if (pos_ + 6 > text_.size() || text_[pos_] != '\\' ||
                  text_[pos_ + 1] != 'u') {
                return false;
              }
              uint32_t low = 0;
              for (size_t i = 2; i <= 5; ++i) {
                const int digit = HexDigit(text_[pos_ + i]);
                if (digit < 0) {
                  return false;
                }
                low = (low << 4) | static_cast<uint32_t>(digit);
              }
              if (low < 0xDC00 || low > 0xDFFF) {
                return false;
              }
              pos_ += 6;
            } else if (code >= 0xDC00 && code <= 0xDFFF) {
              return false;
            }
            continue;
          }
          default:
            return false;
        }
      }
      ++pos_;
    }
    return false;
  }

  [[nodiscard]] bool Number() {
    const size_t start = pos_;
    if (pos_ < text_.size() && text_[pos_] == '-') {
      ++pos_;
    }
    // int part: either "0" or [1-9][0-9]*
    if (pos_ >= text_.size() ||
        std::isdigit(static_cast<unsigned char>(text_[pos_])) == 0) {
      return false;
    }
    if (text_[pos_] == '0') {
      ++pos_;
    } else {
      while (pos_ < text_.size() &&
             std::isdigit(static_cast<unsigned char>(text_[pos_])) != 0) {
        ++pos_;
      }
    }
    if (pos_ < text_.size() && text_[pos_] == '.') {
      ++pos_;
      if (pos_ >= text_.size() ||
          std::isdigit(static_cast<unsigned char>(text_[pos_])) == 0) {
        return false;
      }
      while (pos_ < text_.size() &&
             std::isdigit(static_cast<unsigned char>(text_[pos_])) != 0) {
        ++pos_;
      }
    }
    if (pos_ < text_.size() && (text_[pos_] == 'e' || text_[pos_] == 'E')) {
      ++pos_;
      if (pos_ < text_.size() && (text_[pos_] == '+' || text_[pos_] == '-')) {
        ++pos_;
      }
      if (pos_ >= text_.size() ||
          std::isdigit(static_cast<unsigned char>(text_[pos_])) == 0) {
        return false;
      }
      while (pos_ < text_.size() &&
             std::isdigit(static_cast<unsigned char>(text_[pos_])) != 0) {
        ++pos_;
      }
    }
    return pos_ > start;
  }

  void SkipSpace() {
    while (pos_ < text_.size()) {
      const char c = text_[pos_];
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
        ++pos_;
      } else {
        break;
      }
    }
  }

  std::string_view text_;
  size_t pos_{0};
  int depth_{0};
};

inline bool IsValid(std::string_view text) { return Validator(text).Valid(); }

// Renders a Value in JSON text form (TO_JSON semantics). VARCHAR is either a
// struct payload (leading '{' — kept verbatim so TO_JSON over the engine's
// anonymous-struct representation round-trips) or a quoted string; numerics
// and booleans render bare; arrays recurse. A NULL Value renders as `null`;
// SQL NULL handling stays with the callers.
inline std::string ToJsonText(const Value& value) {
  switch (value.type) {
    case ValueType::kVarChar: {
      const std::string_view text = value.value.varchar_value;
      if (text.size() >= 2 && text.front() == '{' && text.back() == '}') {
        return std::string(text);
      }
      return EncodeJsonString(text);
    }
    case ValueType::kInt64:
      if (value.IsUnsigned() && value.value.int_value < 0) {
        // Unsigned values above INT64_MAX carry no sign bit; render the
        // full magnitude instead of a negative wrap.
        const auto magnitude = static_cast<uint64_t>(value.value.int_value);
        return std::to_string(magnitude);
      }
      return value.AsString();
    case ValueType::kDouble:
      return FormatDoubleShortest(value.value.double_value);
    case ValueType::kArray: {
      std::string out = "[";
      bool first = true;
      for (const Value& element : value.ArrayElements()) {
        if (!first) {
          out += ",";
        }
        first = false;
        out += ToJsonText(element);
      }
      out += "]";
      return out;
    }
    default:
      return EncodeJsonString(value.AsString());
  }
}

}  // namespace tinylamb::json_text

#endif  // TINYLAMB_JSON_TEXT_HPP
