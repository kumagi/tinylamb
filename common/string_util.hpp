/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_STRING_UTIL_HPP
#define TINYLAMB_COMMON_STRING_UTIL_HPP

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb {

[[nodiscard]] constexpr char AsciiToLower(char c) noexcept {
  return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

[[nodiscard]] constexpr char AsciiToUpper(char c) noexcept {
  return (c >= 'a' && c <= 'z') ? static_cast<char>(c - 'a' + 'A') : c;
}

[[nodiscard]] inline std::string AsciiToLower(std::string_view text) {
  std::string out(text);
  for (char& c : out) {
    c = AsciiToLower(c);
  }
  return out;
}

[[nodiscard]] inline std::string AsciiToUpper(std::string_view text) {
  std::string out(text);
  for (char& c : out) {
    c = AsciiToUpper(c);
  }
  return out;
}

// Case-insensitive SQL identifier equality with an ASCII fast path and
// std::tolower fallback for non-ASCII bytes.
[[nodiscard]] inline bool IdentifierEquals(std::string_view left,
                                           std::string_view right) noexcept {
  if (left == right) [[likely]] {
    return true;
  }
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); ++i) {
    const auto lhs = static_cast<unsigned char>(left[i]);
    const auto rhs = static_cast<unsigned char>(right[i]);
    if (lhs == rhs) {
      continue;
    }
    if (lhs >= 0x80 || rhs >= 0x80) {
      if (std::tolower(lhs) != std::tolower(rhs)) {
        return false;
      }
      continue;
    }
    const unsigned char folded_left =
        lhs >= 'A' && lhs <= 'Z' ? lhs + ('a' - 'A') : lhs;
    const unsigned char folded_right =
        rhs >= 'A' && rhs <= 'Z' ? rhs + ('a' - 'A') : rhs;
    if (folded_left != folded_right) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] inline bool ContainsIdentifier(
    const std::vector<std::string>& names, std::string_view name) noexcept {
  return std::ranges::any_of(names, [&](const std::string& candidate) {
    return IdentifierEquals(candidate, name);
  });
}

[[nodiscard]] constexpr std::string_view TrimAsciiWhitespace(
    std::string_view text) noexcept {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t' ||
                           text.front() == '\n' || text.front() == '\r')) {
    text.remove_prefix(1);
  }
  while (!text.empty() && (text.back() == ' ' || text.back() == '\t' ||
                           text.back() == '\n' || text.back() == '\r')) {
    text.remove_suffix(1);
  }
  return text;
}

// Byte length of the UTF-8 sequence led by `lead` (1 for ASCII and for any
// byte that cannot start a sequence, so malformed input degrades to bytes).
[[nodiscard]] constexpr size_t Utf8SequenceLength(unsigned char lead) noexcept {
  if (lead < 0x80U) {
    return 1;
  }
  if ((lead & 0xE0U) == 0xC0U) {
    return 2;
  }
  if ((lead & 0xF0U) == 0xE0U) {
    return 3;
  }
  if ((lead & 0xF8U) == 0xF0U) {
    return 4;
  }
  return 1;
}

// GoogleSQL TRIM / LTRIM / RTRIM: strip leading and/or trailing CODE POINTS
// that occur in `cutset`. Matching individual bytes of a multi-byte cutset
// cut the lead byte of an unrelated character ('Êx' trimmed by 'É' left an
// invalid 0xAA). Malformed sequences are compared byte by byte.
[[nodiscard]] inline std::string_view TrimCodePoints(std::string_view text,
                                                     std::string_view cutset,
                                                     bool leading,
                                                     bool trailing) {
  std::vector<std::string_view> points;
  for (size_t i = 0; i < cutset.size();) {
    const size_t len =
        std::min(Utf8SequenceLength(static_cast<unsigned char>(cutset[i])),
                 cutset.size() - i);
    points.push_back(cutset.substr(i, len));
    i += len;
  }
  const auto in_cutset = [&](std::string_view point) {
    return std::ranges::find(points, point) != points.end();
  };
  if (leading) {
    while (!text.empty()) {
      const size_t len =
          std::min(Utf8SequenceLength(static_cast<unsigned char>(text[0])),
                   text.size());
      if (!in_cutset(text.substr(0, len))) {
        break;
      }
      text.remove_prefix(len);
    }
  }
  if (trailing) {
    while (!text.empty()) {
      // Back up over continuation bytes to the start of the last sequence.
      size_t start = text.size() - 1;
      while (start > 0 && text.size() - start < 4 &&
             (static_cast<unsigned char>(text[start]) & 0xC0U) == 0x80U) {
        --start;
      }
      size_t len = text.size() - start;
      if (Utf8SequenceLength(static_cast<unsigned char>(text[start])) != len) {
        start = text.size() - 1;  // malformed tail: compare the last byte
        len = 1;
      }
      if (!in_cutset(text.substr(start, len))) {
        break;
      }
      text.remove_suffix(len);
    }
  }
  return text;
}

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_STRING_UTIL_HPP
