/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_JSON_STRING_HPP
#define TINYLAMB_COMMON_JSON_STRING_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace tinylamb::json_text {

// Appends `text` as a quoted, escaped JSON string literal to `out`.
inline void AppendJsonString(std::string& out, std::string_view text) {
  out.push_back('"');
  for (const char c : text) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\b':
        out += "\\b";
        break;
      case '\f':
        out += "\\f";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          constexpr std::string_view hex = "0123456789abcdef";
          const auto byte = static_cast<unsigned char>(c);
          out += "\\u00";
          out.push_back(hex[byte >> 4]);
          out.push_back(hex[byte & 0x0fU]);
        } else {
          out.push_back(c);
        }
        break;
    }
  }
  out.push_back('"');
}

inline std::string EncodeJsonString(std::string_view text) {
  std::string out;
  out.reserve(text.size() + 2);
  AppendJsonString(out, text);
  return out;
}

// Decode one complete JSON string, including UTF-16 escape pairs.
inline std::optional<std::string> DecodeJsonString(std::string_view text) {
  if (text.size() < 2 || text.front() != '"' || text.back() != '"') {
    return std::nullopt;
  }
  std::string out;
  size_t offset = 1;
  const size_t end = text.size() - 1;
  const auto hex4 = [&]() -> std::optional<uint32_t> {
    if (end - offset < 4) {
      return std::nullopt;
    }
    uint32_t code = 0;
    for (size_t j = 0; j < 4; ++j) {
      const char digit = text[offset++];
      uint32_t value = 0;
      if (digit >= '0' && digit <= '9') {
        value = static_cast<uint32_t>(digit - '0');
      } else if (digit >= 'a' && digit <= 'f') {
        value = static_cast<uint32_t>(digit - 'a' + 10);
      } else if (digit >= 'A' && digit <= 'F') {
        value = static_cast<uint32_t>(digit - 'A' + 10);
      } else {
        return std::nullopt;
      }
      code = (code << 4) | value;
    }
    return code;
  };
  while (offset < end) {
    const char c = text[offset++];
    if (c == '"' || static_cast<unsigned char>(c) < 0x20) {
      return std::nullopt;
    }
    if (c != '\\') {
      out.push_back(c);
      continue;
    }
    if (offset == end) {
      return std::nullopt;
    }
    switch (text[offset++]) {
      case '"':
        out.push_back('"');
        break;
      case '\\':
        out.push_back('\\');
        break;
      case '/':
        out.push_back('/');
        break;
      case 'b':
        out.push_back('\b');
        break;
      case 'f':
        out.push_back('\f');
        break;
      case 'n':
        out.push_back('\n');
        break;
      case 'r':
        out.push_back('\r');
        break;
      case 't':
        out.push_back('\t');
        break;
      case 'u': {
        auto code = hex4();
        if (!code) {
          return std::nullopt;
        }
        if (*code >= 0xD800 && *code <= 0xDBFF) {
          if (end - offset < 6 || text.substr(offset, 2) != "\\u") {
            return std::nullopt;
          }
          offset += 2;
          const auto low = hex4();
          if (!low || *low < 0xDC00 || *low > 0xDFFF) {
            return std::nullopt;
          }
          *code = 0x10000 + (((*code - 0xD800) << 10) | (*low - 0xDC00));
        } else if (*code >= 0xDC00 && *code <= 0xDFFF) {
          return std::nullopt;
        }
        if (*code < 0x80) {
          out.push_back(static_cast<char>(*code));
        } else if (*code < 0x800) {
          out.push_back(static_cast<char>(0xC0 | (*code >> 6)));
          out.push_back(static_cast<char>(0x80 | (*code & 0x3F)));
        } else if (*code < 0x10000) {
          out.push_back(static_cast<char>(0xE0 | (*code >> 12)));
          out.push_back(static_cast<char>(0x80 | ((*code >> 6) & 0x3F)));
          out.push_back(static_cast<char>(0x80 | (*code & 0x3F)));
        } else {
          out.push_back(static_cast<char>(0xF0 | (*code >> 18)));
          out.push_back(static_cast<char>(0x80 | ((*code >> 12) & 0x3F)));
          out.push_back(static_cast<char>(0x80 | ((*code >> 6) & 0x3F)));
          out.push_back(static_cast<char>(0x80 | (*code & 0x3F)));
        }
        break;
      }
      default:
        return std::nullopt;
    }
  }
  return out;
}

}  // namespace tinylamb::json_text

#endif  // TINYLAMB_COMMON_JSON_STRING_HPP
