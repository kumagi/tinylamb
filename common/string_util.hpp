/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_STRING_UTIL_HPP
#define TINYLAMB_COMMON_STRING_UTIL_HPP

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <span>
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
// byte that cannot start a VALID sequence, so malformed input degrades to
// bytes).  T1542: 0xC0/0xC1 (overlong 2-byte forms) and 0xF5..0xF7 (lead
// bytes beyond U+10FFFF) do start sequences under the old masking, so TRIM
// cutsets and LIKE `_` consumed them as one character while the canonical
// validator (IsValidUtf8Text) rejects exactly those leads.  Acceptance is
// now the same C2..DF / E0..EF / F0..F4 window.
[[nodiscard]] constexpr size_t Utf8SequenceLength(unsigned char lead) noexcept {
  if (lead < 0x80U) {
    return 1;
  }
  if (lead >= 0xC2U && lead <= 0xDFU) {
    return 2;
  }
  if ((lead & 0xF0U) == 0xE0U) {
    return 3;
  }
  if (lead >= 0xF0U && lead <= 0xF4U) {
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
  // Membership structure: one-byte points go into a stack 256-bit bitmap,
  // multi-byte points into a small inline array (a rare overflow vector
  // covers pathological cutsets).  The previous per-call
  // vector<string_view> + linear std::ranges::find cost a malloc and
  // O(text points x cutset points) comparisons on every trimmed row, even
  // for the six ASCII bytes of the default cutset.
  std::array<uint64_t, 4> single_byte_bits{};
  std::array<std::string_view, 8> multi_byte_points{};
  size_t multi_byte_count = 0;
  std::vector<std::string_view> overflow_points;
  for (size_t i = 0; i < cutset.size();) {
    const size_t len =
        std::min(Utf8SequenceLength(static_cast<unsigned char>(cutset[i])),
                 cutset.size() - i);
    const std::string_view point = cutset.substr(i, len);
    if (point.size() == 1) {
      const auto byte = static_cast<unsigned char>(point[0]);
      single_byte_bits[byte >> 6] |= uint64_t{1} << (byte & 63U);
    } else if (multi_byte_count < multi_byte_points.size()) {
      multi_byte_points[multi_byte_count++] = point;
    } else {
      overflow_points.push_back(point);
    }
    i += len;
  }
  const auto in_cutset = [&](std::string_view point) -> bool {
    if (point.size() == 1) {
      const auto byte = static_cast<unsigned char>(point[0]);
      return ((single_byte_bits[byte >> 6] >> (byte & 63U)) & 1U) != 0U;
    }
    for (const std::string_view& known :
         std::span(multi_byte_points).first(multi_byte_count)) {
      if (known == point) {
        return true;
      }
    }
    return std::ranges::find(overflow_points, point) != overflow_points.end();
  };
  // A cutset byte that is not a complete sequence (e.g. a lone 0xAC from a
  // BYTES literal) only ever forms a one-byte point. Match it byte by byte in
  // the text even when the text byte is part of a well-formed sequence, so a
  // BYTES cutset keeps byte semantics while a valid multi-byte STRING cutset
  // keeps code-point semantics.
  if (leading) {
    while (!text.empty()) {
      const size_t len = std::min(
          Utf8SequenceLength(static_cast<unsigned char>(text[0])), text.size());
      if (in_cutset(text.substr(0, len))) {
        text.remove_prefix(len);
        continue;
      }
      if (len > 1 && in_cutset(text.substr(0, 1))) {
        text.remove_prefix(1);
        continue;
      }
      break;
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
      if (in_cutset(text.substr(start, len))) {
        text.remove_suffix(len);
        continue;
      }
      if (len > 1 && in_cutset(text.substr(text.size() - 1, 1))) {
        text.remove_suffix(1);
        continue;
      }
      break;
    }
  }
  return text;
}

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_STRING_UTIL_HPP
