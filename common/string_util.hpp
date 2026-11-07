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

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_STRING_UTIL_HPP
