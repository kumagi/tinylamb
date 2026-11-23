/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_STRING_UTIL_HPP
#define TINYLAMB_COMMON_STRING_UTIL_HPP

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb {

// An empty search target matches all code-point boundaries, including the
// boundary after the last character. Position is nonzero and occurrence is
// positive. A negative position is one-based from the end (`-1` starts at the
// last character), matching the non-empty-target search.
[[nodiscard]] inline int64_t InstrEmptyTargetPosition(size_t length,
                                                      int64_t position,
                                                      int64_t occurrence) {
  const uint64_t start =
      position > 0 ? static_cast<uint64_t>(position - 1)
                   : static_cast<uint64_t>(-(position + 1)) + uint64_t{1};
  const auto extra = static_cast<uint64_t>(occurrence - 1);
  if (start > length || extra > length - start) {
    return 0;
  }
  const auto offset = static_cast<size_t>(start + extra);
  const size_t index = position > 0 ? offset : length - offset;
  return index < static_cast<uint64_t>(std::numeric_limits<int64_t>::max())
             ? static_cast<int64_t>(index) + 1
             : 0;
}

// Appends an integer's decimal representation without allocating a temporary
// std::string. std::to_chars writes into a stack buffer, so repeated use in a
// loop (e.g. building signature/tag strings) performs no per-call heap
// allocation. The fallback keeps the helper valid for exotic integer types.
template <typename T>
inline void AppendInteger(std::string& out, T value) {
  std::array<char, static_cast<size_t>(std::numeric_limits<T>::digits10) + 3>
      buffer{};
  const auto result =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  if (result.ec == std::errc{}) {
    out.append(buffer.data(), result.ptr);
  } else {
    out += std::to_string(value);
  }
}

// std::to_string equivalent that avoids the temporary heap buffer for the
// integer types used in hot signature-building paths.
template <typename T>
[[nodiscard]] inline std::string IntegerToString(T value) {
  std::string out;
  AppendInteger(out, value);
  return out;
}

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

// Canonical UTF-8 well-formedness check: rejects overlong forms, surrogate
// halves (ED A0..BF), code points beyond U+10FFFF (F4 90+), stray
// continuation bytes and truncated sequences.  ASCII control bytes are
// valid UTF-8 (callers that need printable text check separately).
[[nodiscard]] inline bool IsValidUtf8Text(std::string_view text) {
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
    if (lead >= 0xC2 && lead <= 0xDF) {
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

// GoogleSQL TRIM / LTRIM / RTRIM: strip leading and/or trailing CODE POINTS
// that occur in `cutset`. Matching individual bytes of a multi-byte cutset
// cut the lead byte of an unrelated character ('Êx' trimmed by 'É' left an
// invalid 0xAA). Malformed sequences are compared byte by byte.
//
// The DEFAULT cutset (one-argument TRIM) is every Unicode White_Space code
// point, not just the six ASCII whitespace characters: NEL, NBSP, ogham
// space, U+2000-U+200A, line/paragraph separators, narrow NBSP, medium
// math space and ideographic space included.  Multi-byte members stay
// intact because TrimCodePoints matches whole code points.
inline constexpr char kDefaultTrimCutset[] =
    " \t\n\v\f\r"
    "\xC2\x85"      // U+0085 NEXT LINE
    "\xC2\xA0"      // U+00A0 NO-BREAK SPACE
    "\xE1\x9A\x80"  // U+1680 OGHAM SPACE MARK
    "\xE2\x80\x80\xE2\x80\x81\xE2\x80\x82\xE2\x80\x83\xE2\x80\x84"
    "\xE2\x80\x85\xE2\x80\x86\xE2\x80\x87\xE2\x80\x88\xE2\x80\x89"
    "\xE2\x80\x8A"              // U+2000..U+200A
    "\xE2\x80\xA8\xE2\x80\xA9"  // U+2028 LINE / U+2029 PARAGRAPH SEPARATOR
    "\xE2\x80\xAF"              // U+202F NARROW NO-BREAK SPACE
    "\xE2\x81\x9F"              // U+205F MEDIUM MATHEMATICAL SPACE
    "\xE3\x80\x80";             // U+3000 IDEOGRAPHIC SPACE

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
      if (overflow_points.empty()) {
        overflow_points.reserve(16);
      }
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
  const auto contains_single_byte_cutset_member = [&](std::string_view point) {
    return std::ranges::any_of(point, [&](char c) {
      const auto byte = static_cast<unsigned char>(c);
      return ((single_byte_bits[byte >> 6] >> (byte & 63U)) & 1U) != 0U;
    });
  };
  // A cutset byte that is not a complete sequence (e.g. a lone 0xAC from a
  // BYTES literal) matches a valid edge code point when any of its bytes
  // matches. Remove the complete code point so trimming does not leave broken
  // UTF-8. Malformed text still falls back to byte-wise trimming.
  if (leading) {
    while (!text.empty()) {
      const size_t len = std::min(
          Utf8SequenceLength(static_cast<unsigned char>(text[0])), text.size());
      if (in_cutset(text.substr(0, len))) {
        text.remove_prefix(len);
        continue;
      }
      const std::string_view edge = text.substr(0, len);
      const bool complete_sequence =
          len > 1 && std::ranges::all_of(edge.substr(1), [](char c) {
            return (static_cast<unsigned char>(c) & 0xC0U) == 0x80U;
          });
      if (complete_sequence && contains_single_byte_cutset_member(edge)) {
        // Keep a valid UTF-8 edge code point intact. A one-byte BYTES cutset
        // may match any byte within it, but trimming only that byte would
        // leave a malformed prefix and differ from the trailing path.
        text.remove_prefix(len);
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
      if (len > 1 &&
          contains_single_byte_cutset_member(text.substr(start, len))) {
        text.remove_suffix(len);
        continue;
      }
      break;
    }
  }
  return text;
}

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_STRING_UTIL_HPP
