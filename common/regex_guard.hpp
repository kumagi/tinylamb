/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_REGEX_GUARD_HPP
#define TINYLAMB_REGEX_GUARD_HPP

#include <bitset>
#include <cstddef>
#include <cstdint>
#include <regex>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

namespace regex_guard_detail {

using CharSet = std::bitset<256>;

inline void AddRange(CharSet& set, unsigned lo, unsigned hi) {
  for (unsigned c = lo; c <= hi && c < 256; ++c) set.set(c);
}

// Character set denoted by a one-letter escape: "\d", "\w", "\s" and
// their negations, plus the usual control escapes. Anything else maps to
// the escaped literal itself ("\+" is '+'); "\b"/"\B" are zero-width and
// handled by the caller via `nullable`.
inline CharSet EscapeCharSet(char e) {
  CharSet set;
  switch (e) {
    case 'd':
      AddRange(set, '0', '9');
      return set;
    case 'D':
      AddRange(set, '0', '9');
      return ~set;
    case 'w':
      AddRange(set, '0', '9');
      AddRange(set, 'a', 'z');
      AddRange(set, 'A', 'Z');
      set.set('_');
      return set;
    case 'W':
      return ~EscapeCharSet('w');
    case 's':
      for (char c : {' ', '\t', '\n', '\v', '\f', '\r'}) {
        set.set(static_cast<unsigned char>(c));
      }
      return set;
    case 'S':
      return ~EscapeCharSet('s');
    case 'n':
      set.set('\n');
      return set;
    case 't':
      set.set('\t');
      return set;
    case 'r':
      set.set('\r');
      return set;
    case 'f':
      set.set('\f');
      return set;
    case 'v':
      set.set('\v');
      return set;
    default:
      set.set(static_cast<unsigned char>(e));
      return set;
  }
}

// ASCII approximation of a POSIX "[:name:]" class.
inline CharSet PosixClassSet(std::string_view name) {
  CharSet set;
  if (name == "digit") AddRange(set, '0', '9');
  if (name == "xdigit") {
    AddRange(set, '0', '9');
    AddRange(set, 'a', 'f');
    AddRange(set, 'A', 'F');
  }
  if (name == "alpha" || name == "alnum") {
    AddRange(set, 'a', 'z');
    AddRange(set, 'A', 'Z');
  }
  if (name == "alnum" || name == "digit") AddRange(set, '0', '9');
  if (name == "lower") AddRange(set, 'a', 'z');
  if (name == "upper") AddRange(set, 'A', 'Z');
  if (name == "space" || name == "blank") set.set(' ');
  if (name == "space") {
    for (char c : {'\t', '\n', '\v', '\f', '\r'}) {
      set.set(static_cast<unsigned char>(c));
    }
  }
  if (name == "blank") set.set('\t');
  if (name == "cntrl") {
    AddRange(set, 0, 31);
    set.set(127);
  }
  if (name == "print") AddRange(set, 32, 126);
  if (name == "graph") AddRange(set, 33, 126);
  if (name == "punct") {
    AddRange(set, 33, 47);
    AddRange(set, 58, 64);
    AddRange(set, 91, 96);
    AddRange(set, 123, 126);
  }
  return set;
}

}  // namespace regex_guard_detail

// std::regex uses a DFS backtracking executor, so an unbounded quantifier
// applied to an ambiguous sub-expression can take exponential time while
// tiling a non-matching subject (a REGEXP_REPLACE fuzz probe hung for
// >20 minutes on a 75-byte string). Two ambiguity shapes are caught here:
//
//   * a variable-length operand — "(a+)+", ".++", "(a?)+", "a+{2,}*",
//     "(a|bc)*"; and
//   * an alternation whose branches share a first character — "(a|a)+",
//     "(x|xy)*", "(\d|\w)+".
//
// The check is conservative: an unbounded quantifier (`*`, `+`, `{m,}`)
// over such an operand is rejected, while lazy modifiers ("a+?"), bounded
// counts ("a{2,4}", "(a+){3}") and sequential repetitions ("a.*b.*c")
// stay allowed. Linear-time engines (RE2) do not need this.
inline bool RegexPatternHasNestedRepetition(std::string_view pattern) {
  using regex_guard_detail::CharSet;
  constexpr int64_t kVar = -1;  // variable-length element marker

  struct Frame {
    // Per open '(' group: for every alternation branch, its fixed length
    // (kVar when variable), the set of possible first characters, whether
    // it can match empty, and whether it already contains an ambiguous
    // alternation (propagated outward so "((a|a)b)+" is still caught).
    std::vector<int64_t> branch_lens;
    std::vector<CharSet> branch_firsts;
    std::vector<bool> branch_nullable;
    std::vector<bool> branch_ambiguous;
    int64_t cur_len = 0;
    CharSet cur_first;
    bool first_done = false;  // a non-nullable element already seen
    bool cur_nullable = true;
    bool cur_ambiguous = false;
  };
  std::vector<Frame> groups;

  // The element in progress: its fixed length (kVar = variable), the set
  // of characters it can start with, whether it can match the empty
  // string, and — for groups — whether its alternation branches overlap.
  // Length stays pending until the element is complete so a quantifier
  // can rewrite it ("a{3}" contributes 3, not 1 + 3).
  int64_t pending_len = 0;
  CharSet pending_first;
  bool pending_nullable = true;
  // Whether the operand itself is dangerous under an unbounded quantifier:
  // it contains a variable-length element (pending_var) or an alternation
  // with overlapping branches (pending_ambiguous). Distinct fixed-length
  // branches — "(cat|dog|bird)" — are deterministic and stay allowed.
  bool pending_var = false;
  bool pending_ambiguous = false;
  bool elem_quantified = false;  // trailing '?' is a laziness modifier

  const auto flush = [&] {
    if (!groups.empty()) {
      Frame& f = groups.back();
      f.cur_len = (f.cur_len == kVar || pending_len == kVar)
                      ? kVar
                      : f.cur_len + pending_len;
      if (!f.first_done) {
        f.cur_first |= pending_first;
        if (!pending_nullable) f.first_done = true;
      }
      if (!pending_nullable) f.cur_nullable = false;
      f.cur_ambiguous = f.cur_ambiguous || pending_ambiguous;
    }
    pending_len = 0;
    pending_first.reset();
    pending_nullable = true;
    pending_var = false;
    pending_ambiguous = false;
  };

  const auto note_atom = [&](int64_t len, CharSet first, bool nullable) {
    flush();
    pending_len = len;
    pending_first = first;
    pending_nullable = nullable;
    pending_var = false;
    pending_ambiguous = false;
    elem_quantified = false;
  };

  size_t i = 0;
  const size_t n = pattern.size();
  while (i < n) {
    const char c = pattern[i];
    if (c == '\\') {
      CharSet first;
      bool nullable = false;
      if (i + 1 < n && (pattern[i + 1] == 'b' || pattern[i + 1] == 'B')) {
        nullable = true;  // word boundary: zero-width
      } else if (i + 1 < n) {
        first = regex_guard_detail::EscapeCharSet(pattern[i + 1]);
      }
      i += 2;
      note_atom(1, first, nullable);
      continue;
    }
    if (c == '[') {
      CharSet first;
      ++i;
      const bool negated = i < n && pattern[i] == '^';
      if (negated) ++i;
      bool first_item = true;
      while (i < n && (pattern[i] != ']' || first_item)) {
        first_item = false;
        if (pattern[i] == '\\' && i + 1 < n) {
          const CharSet esc = regex_guard_detail::EscapeCharSet(pattern[i + 1]);
          if (i + 2 < n && pattern[i + 2] == '-' && i + 3 < n &&
              pattern[i + 3] != ']') {
            // Escape endpoints of ranges are rare and complicated; treat
            // the range as fully covered rather than misjudging overlap.
            first.set();
            i += 4;
            continue;
          }
          first |= esc;
          i += 2;
          continue;
        }
        if (pattern[i] == '[' && i + 1 < n &&
            (pattern[i + 1] == ':' || pattern[i + 1] == '.' ||
             pattern[i + 1] == '=')) {
          const char kind = pattern[i + 1];
          size_t j = i + 2;
          while (j + 1 < n && !(pattern[j] == kind && pattern[j + 1] == ']')) {
            ++j;
          }
          if (j + 1 < n) {
            if (kind == ':') {
              first |= regex_guard_detail::PosixClassSet(
                  pattern.substr(i + 2, j - i - 2));
            } else if (i + 2 < j) {
              first.set(static_cast<unsigned char>(pattern[i + 2]));
            }
            i = j + 2;
            continue;
          }
          // Unterminated "[.x" — fall through and treat '[' literally.
        }
        if (i + 2 < n && pattern[i + 1] == '-' && pattern[i + 2] != ']') {
          regex_guard_detail::AddRange(
              first, static_cast<unsigned char>(pattern[i]),
              static_cast<unsigned char>(pattern[i + 2]));
          i += 3;
          continue;
        }
        first.set(static_cast<unsigned char>(pattern[i]));
        ++i;
      }
      if (i < n) ++i;  // consume ']'
      if (negated) first.flip();
      note_atom(1, first, false);
      continue;
    }
    if (c == '(') {
      flush();
      groups.push_back(Frame{});
      pending_len = 0;
      pending_first.reset();
      pending_nullable = true;
      pending_var = false;
      pending_ambiguous = false;
      elem_quantified = false;
      ++i;
      if (i < n && pattern[i] == '?') {
        // Group-type marker, not a quantifier: (?: (?= (?! (?<name> (?<=
        // (?<! — consume it here so '?' is not misread as one.
        ++i;
        if (i < n &&
            (pattern[i] == ':' || pattern[i] == '=' || pattern[i] == '!')) {
          ++i;
        } else if (i < n && pattern[i] == '<') {
          ++i;
          if (i < n && (pattern[i] == '=' || pattern[i] == '!')) {
            ++i;
          } else {
            while (i < n && pattern[i] != '>') ++i;  // (?<name>
            if (i < n) ++i;
          }
        }
      }
      continue;
    }
    if (c == ')') {
      int64_t len = 0;
      CharSet first;
      bool nullable = true;
      bool var_elem = false;
      bool ambiguous = false;
      if (!groups.empty()) {
        nullable = false;
        flush();  // complete the in-progress branch's last element
        Frame f = std::move(groups.back());
        groups.pop_back();
        f.branch_lens.push_back(f.cur_len);
        f.branch_firsts.push_back(f.cur_first);
        f.branch_nullable.push_back(f.cur_nullable);
        f.branch_ambiguous.push_back(f.cur_ambiguous);
        bool var = false;
        for (size_t b = 0; b < f.branch_lens.size(); ++b) {
          if (f.branch_lens[b] == kVar) {
            var = true;
            var_elem = true;  // branch contains a variable-length element
          } else if (f.branch_lens[b] != f.branch_lens[0]) {
            var = true;  // distinct fixed lengths: bookkeeping only
          }
          nullable = nullable || f.branch_nullable[b];
          ambiguous = ambiguous || f.branch_ambiguous[b];
          first |= f.branch_firsts[b];
          for (size_t a = 0; a < b; ++a) {
            if ((f.branch_firsts[a] & f.branch_firsts[b]).any()) {
              ambiguous = true;
            }
          }
        }
        len = var ? kVar : f.branch_lens[0];
      }
      ++i;
      pending_len = len;
      pending_first = first;
      pending_nullable = nullable;
      pending_var = var_elem;
      pending_ambiguous = ambiguous;
      elem_quantified = false;
      continue;
    }
    if (c == '|') {
      flush();
      if (!groups.empty()) {
        Frame& f = groups.back();
        f.branch_lens.push_back(f.cur_len);
        f.branch_firsts.push_back(f.cur_first);
        f.branch_nullable.push_back(f.cur_nullable);
        f.branch_ambiguous.push_back(f.cur_ambiguous);
        f.cur_len = 0;
        f.cur_first.reset();
        f.first_done = false;
        f.cur_nullable = true;
        f.cur_ambiguous = false;
      }
      elem_quantified = false;
      ++i;
      continue;
    }
    if (c == '^' || c == '$') {
      ++i;
      note_atom(0, CharSet{}, true);  // zero-width marker
      continue;
    }

    // Quantifiers: '*', '+', '?' and '{m}'/'{m,}'/'{m,n}'. Only a bare
    // '{m}' keeps a fixed length; '{m,}' is the unbounded form.
    bool unbounded = false;
    bool exact = false;
    bool can_be_empty = false;
    int64_t repeat = 0;
    size_t len = 0;  // 0 means c is an ordinary atom, not a quantifier
    if (c == '*') {
      unbounded = true;
      can_be_empty = true;
      len = 1;
    } else if (c == '+') {
      unbounded = true;
      len = 1;
    } else if (c == '?') {
      if (elem_quantified) {
        ++i;  // laziness modifier: part of the previous quantifier
        continue;
      }
      can_be_empty = true;
      len = 1;  // '?' is bounded but length-varying (0 or 1)
    } else if (c == '{') {
      size_t j = i + 1;
      while (j < n && pattern[j] >= '0' && pattern[j] <= '9') ++j;
      if (j > i + 1) {
        int64_t lo = 0;
        for (size_t d = i + 1; d < j; ++d) {
          lo = lo * 10 + (pattern[d] - '0');
          if (lo > (1 << 20)) break;  // saturating; only ordering matters
        }
        if (j < n && pattern[j] == '}') {
          exact = true;
          repeat = lo;
          can_be_empty = (lo == 0);
          len = j - i + 1;
        } else if (j < n && pattern[j] == ',') {
          size_t k = j + 1;
          while (k < n && pattern[k] >= '0' && pattern[k] <= '9') ++k;
          if (k < n && pattern[k] == '}') {
            unbounded = (k == j + 1);  // "{m,}"
            can_be_empty = (lo == 0);
            len = k - i + 1;
          }
        }
      }
    }
    if (len == 0) {
      ++i;  // ordinary atom (incl. a malformed '{')
      CharSet first;
      if (c == '.') {
        first.set();  // '.' matches anything: overlaps every branch
      } else {
        first.set(static_cast<unsigned char>(c));
      }
      note_atom(1, first, false);
      continue;
    }
    // The quantifier applies to the element in progress. Reject only an
    // unbounded quantifier on an ambiguous operand — a variable-length
    // element, overlapping alternation branches, or an operand that can
    // match both the empty string and real input ("(a|)+"). `?`/`{m,n}`/
    // `{m}` stay allowed on anything (bounded fan-out), as does `*`/`+`/
    // `{m,}` on a deterministic operand ("a+", "(ab)*", "(cat|dog)+").
    if (unbounded && (pending_var || pending_ambiguous ||
                      (pending_nullable && pending_first.any()))) {
      return true;
    }
    pending_len = (exact && pending_len != kVar) ? pending_len * repeat : kVar;
    pending_var = (pending_len == kVar);
    pending_nullable = can_be_empty || pending_nullable;
    elem_quantified = true;
    i += len;
  }
  return false;
}

// Compile `pattern` after the nested-repetition check above; callers get a
// uniform StatusError for both rejected and malformed patterns.
inline StatusOr<std::regex> CompileCheckedRegex(const std::string& pattern) {
  if (RegexPatternHasNestedRepetition(pattern)) {
    return StatusError(StatusCode::kInvalidArgument,
                       "regular expression has nested repetition: " + pattern);
  }
  try {
    return std::regex(pattern);
  } catch (...) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid regular expression: " + pattern);
  }
}

}  // namespace tinylamb

#endif  // TINYLAMB_REGEX_GUARD_HPP
