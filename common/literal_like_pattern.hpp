/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_LITERAL_LIKE_PATTERN_HPP
#define TINYLAMB_COMMON_LITERAL_LIKE_PATTERN_HPP
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/platform/process.hpp"
#include "common/string_util.hpp"

namespace tinylamb {

// Runtime toggle for benchmarking / regression verification of fast LIKE paths.
// Lean proofs:
//   - lean/TinylambOpt/LikeSegmentChain.lean (anchored_chain_iff)
//   - lean/TinylambOpt/LikeUnderscore.lean (raw_like_fast_sound)
//   - lean/TinylambOpt/LikeShiftAnd.lean (shiftAnd_iff)
inline std::atomic<bool>& FastLikeDisabledFlag() noexcept {
  static std::atomic<bool> flag = [] {
    const char* env =
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_FAST_LIKE");
    return env != nullptr && env[0] == '1';
  }();
  return flag;
}

inline void SetFastLikeDisabledForTest(bool disabled) noexcept {
  FastLikeDisabledFlag().store(disabled, std::memory_order_relaxed);
}

inline void ReloadFastLikeDisabledFromEnv() noexcept {
  const char* env = ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_FAST_LIKE");
  FastLikeDisabledFlag().store(env != nullptr && env[0] == '1',
                               std::memory_order_relaxed);
}

// Reference UTF-8 / escape-aware LIKE matcher (semantic ground truth).
inline bool EscapedLikeMatchUtf8(std::string_view value,
                                 std::string_view pattern) {
  size_t v = 0, p = 0, wildcard = std::string_view::npos, retry = 0;
  while (v < value.size()) {
    if (p < pattern.size() && pattern[p] == '%') {
      wildcard = ++p;
      retry = v;
    } else if (p < pattern.size() && pattern[p] == '_') {
      v += std::min(Utf8SequenceLength(static_cast<unsigned char>(value[v])),
                    value.size() - v);
      ++p;
    } else {
      size_t next = p;
      if (next < pattern.size() && pattern[next] == '\\' &&
          next + 1 < pattern.size()) {
        ++next;
      }
      if (next < pattern.size() && pattern[next] == value[v]) {
        ++v;
        p = next + 1;
      } else if (wildcard != std::string_view::npos && retry < value.size()) {
        retry += std::min(
            Utf8SequenceLength(static_cast<unsigned char>(value[retry])),
            value.size() - retry);
        v = retry;
        p = wildcard;
      } else {
        return false;
      }
    }
  }
  while (p < pattern.size() && pattern[p] == '%') {
    ++p;
  }
  return p == pattern.size();
}

// Compiled LIKE matcher integrating:
// 1. Multi-segment anchored chain search (LikeSegmentChain.lean:
//    anchored_chain_iff)
// 2. ASCII 1-byte '_' fast path with whole-row UTF-8 fallback
//    (LikeUnderscore.lean: raw_like_fast_sound)
// 3. Bit-parallel Shift-And search for <=64-byte literal/'_' interior segments
//    with per-row reset (LikeShiftAnd.lean: shiftAnd_iff)
struct CompiledLikePattern {
  static constexpr size_t kShiftAndAlphabetSize = 128;

  struct Token {
    uint8_t byte{0};
    bool is_wild{false};
  };
  struct WildSegment {
    std::vector<Token> tokens;
    bool use_shift_and{false};
    uint64_t accept_bit{0};
    std::array<uint64_t, kShiftAndAlphabetSize> sa_mask{};
  };

  std::string source;
  bool fallback_only{false};
  bool has_any_percent{false};
  bool has_underscore{false};
  bool head_anchor{false};
  bool tail_anchor{false};

  // Pure literal chain segments (used when !has_underscore && !fallback_only).
  std::string head_literal;
  std::string tail_literal;
  std::vector<std::string> interior_literals;

  // Mixed literal/'_' segments (used when has_underscore && !fallback_only).
  std::vector<Token> head_wild;
  std::vector<Token> tail_wild;
  std::vector<WildSegment> interior_wild;

  [[nodiscard]] static bool IsAllAscii(std::string_view v) noexcept {
    return std::ranges::all_of(v, [](char ch) {
      return static_cast<uint8_t>(ch) < kShiftAndAlphabetSize;
    });
  }

  [[nodiscard]] static bool MatchWildAt(
      std::string_view data, const std::vector<Token>& seg) noexcept {
    for (size_t i = 0; i < seg.size(); ++i) {
      if (!seg[i].is_wild && static_cast<uint8_t>(data[i]) != seg[i].byte) {
        return false;
      }
    }
    return true;
  }

  static WildSegment BuildWildSegment(std::vector<Token> tokens) {
    WildSegment seg;
    seg.tokens = std::move(tokens);
    const size_t len = seg.tokens.size();
    if (len >= 1 && len <= 64) {
      seg.use_shift_and = true;
      seg.accept_bit = uint64_t{1} << (len - 1);
      for (size_t j = 0; j < len; ++j) {
        const uint64_t bit = uint64_t{1} << j;
        if (seg.tokens[j].is_wild) {
          for (uint64_t& m : seg.sa_mask) {
            m |= bit;
          }
        } else {
          seg.sa_mask[seg.tokens[j].byte] |= bit;
        }
      }
    }
    return seg;
  }

  static CompiledLikePattern Compile(std::string_view pattern) {
    CompiledLikePattern out;
    out.source.assign(pattern.data(), pattern.size());

    std::vector<std::vector<Token>> segments;
    segments.emplace_back();
    bool saw_percent = false;
    bool leading_percent = false;
    bool prev_was_percent = false;

    for (size_t i = 0; i < pattern.size(); ++i) {
      const char ch = pattern[i];
      if (ch == '%') {
        if (!saw_percent && segments.back().empty()) {
          leading_percent = true;
        }
        saw_percent = true;
        if (!segments.back().empty()) {
          segments.emplace_back();
        }
        prev_was_percent = true;
        continue;
      }
      prev_was_percent = false;
      if (ch == '_') {
        out.has_underscore = true;
        segments.back().push_back(Token{.byte = 0, .is_wild = true});
        continue;
      }
      auto b = static_cast<uint8_t>(ch);
      if (ch == '\\' && i + 1 < pattern.size()) {
        ++i;
        b = static_cast<uint8_t>(pattern[i]);
      }
      if (b >= kShiftAndAlphabetSize) {
        out.fallback_only = true;
        return out;
      }
      segments.back().push_back(Token{.byte = b, .is_wild = false});
    }

    out.has_any_percent = saw_percent;
    const bool trailing_percent = prev_was_percent;
    if (segments.back().empty() && segments.size() > 1) {
      segments.pop_back();
    }
    if (!saw_percent) {
      // Single unsplit pattern (possibly empty).
      if (!out.has_underscore) {
        out.head_literal.reserve(segments[0].size());
        for (const Token& t : segments[0]) {
          out.head_literal.push_back(static_cast<char>(t.byte));
        }
      } else {
        out.head_wild = std::move(segments[0]);
      }
      return out;
    }

    // Remove empty placeholder when pattern consists solely of '%' runs.
    if (segments.size() == 1 && segments[0].empty()) {
      segments.clear();
    }
    if (segments.empty()) {
      return out;
    }

    size_t lo = 0;
    size_t hi = segments.size();
    out.head_anchor = !leading_percent;
    out.tail_anchor = !trailing_percent;

    if (!out.has_underscore) {
      auto to_string = [](const std::vector<Token>& toks) {
        std::string s;
        s.reserve(toks.size());
        for (const Token& t : toks) {
          s.push_back(static_cast<char>(t.byte));
        }
        return s;
      };
      if (out.head_anchor) {
        out.head_literal = to_string(segments[lo++]);
      }
      if (out.tail_anchor) {
        if (lo < hi) {
          out.tail_literal = to_string(segments[--hi]);
        } else {
          out.tail_anchor = false;
        }
      }
      out.interior_literals.reserve(hi - lo);
      for (size_t k = lo; k < hi; ++k) {
        out.interior_literals.push_back(to_string(segments[k]));
      }
    } else {
      if (out.head_anchor) {
        out.head_wild = std::move(segments[lo++]);
      }
      if (out.tail_anchor) {
        if (lo < hi) {
          out.tail_wild = std::move(segments[--hi]);
        } else {
          out.tail_anchor = false;
        }
      }
      out.interior_wild.reserve(hi - lo);
      for (size_t k = lo; k < hi; ++k) {
        out.interior_wild.push_back(BuildWildSegment(std::move(segments[k])));
      }
    }
    return out;
  }

  [[nodiscard]] bool Matches(std::string_view value) const {
    if (fallback_only || !IsAllAscii(value)) {
      // Lean proof: lean/TinylambOpt/LikeUnderscore.lean (raw_like_fast_sound)
      // Fallback restarts from the row head on any non-ASCII byte so '_' and
      // '%' never split multi-byte UTF-8 code points.
      return EscapedLikeMatchUtf8(value, source);
    }

    const size_t n = value.size();
    if (!has_any_percent) {
      if (!has_underscore) {
        return value == head_literal;
      }
      return n == head_wild.size() &&
             MatchWildAt(value.substr(0, head_wild.size()), head_wild);
    }

    size_t pos = 0;
    size_t end = n;
    if (!has_underscore) {
      // Lean proof: lean/TinylambOpt/LikeSegmentChain.lean
      //   (anchored_chain_iff)
      if (head_anchor) {
        if (end - pos < head_literal.size() ||
            !value.starts_with(head_literal)) {
          return false;
        }
        pos += head_literal.size();
      }
      if (tail_anchor) {
        if (end - pos < tail_literal.size() ||
            std::memcmp(value.data() + end - tail_literal.size(),
                        tail_literal.data(), tail_literal.size()) != 0) {
          return false;
        }
        end -= tail_literal.size();
      }
      for (const std::string& seg : interior_literals) {
        if (end - pos < seg.size()) {
          return false;
        }
        const size_t found = value.substr(pos, end - pos).find(seg);
        if (found == std::string_view::npos) {
          return false;
        }
        pos += found + seg.size();
      }
      return true;
    }

    if (head_anchor) {
      if (end - pos < head_wild.size() ||
          !MatchWildAt(value.substr(0, head_wild.size()), head_wild)) {
        return false;
      }
      pos += head_wild.size();
    }
    if (tail_anchor) {
      if (end - pos < tail_wild.size() ||
          !MatchWildAt(value.substr(end - tail_wild.size(), tail_wild.size()),
                       tail_wild)) {
        return false;
      }
      end -= tail_wild.size();
    }
    for (const WildSegment& seg : interior_wild) {
      const size_t slen = seg.tokens.size();
      if (end - pos < slen) {
        return false;
      }
      if (seg.use_shift_and) {
        // Lean proof: lean/TinylambOpt/LikeShiftAnd.lean (shiftAnd_iff)
        // Fresh state = 0 per segment per row (never carried across rows).
        uint64_t state = 0;
        const uint64_t accept = seg.accept_bit;
        bool matched = false;
        for (size_t i = pos; i < end; ++i) {
          state = ((state << 1U) | uint64_t{1}) &
                  seg.sa_mask[static_cast<unsigned char>(value[i])];
          if ((state & accept) != 0) {
            pos = i + 1;
            matched = true;
            break;
          }
        }
        if (!matched) {
          return false;
        }
      } else {
        bool matched = false;
        for (size_t s = pos; s + slen <= end; ++s) {
          if (MatchWildAt(value.substr(s, slen), seg.tokens)) {
            pos = s + slen;
            matched = true;
            break;
          }
        }
        if (!matched) {
          return false;
        }
      }
    }
    return true;
  }
};

// Escape-aware LIKE matcher using a bounded per-thread MRU cache of
// CompiledLikePattern, with fallback to EscapedLikeMatchUtf8.
inline bool EscapedLikeMatch(std::string_view value, std::string_view pattern) {
  if (FastLikeDisabledFlag().load(std::memory_order_relaxed)) {
    return EscapedLikeMatchUtf8(value, pattern);
  }
  struct CacheSlot {
    CompiledLikePattern compiled;
    uint64_t stamp{0};
    bool valid{false};
  };
  static thread_local std::array<CacheSlot, 16> cache;
  static thread_local uint64_t clock = 0;

  for (CacheSlot& slot : cache) {
    if (slot.valid && slot.compiled.source == pattern) {
      slot.stamp = ++clock;
      return slot.compiled.Matches(value);
    }
  }
  size_t victim = 0;
  for (size_t i = 0; i < cache.size(); ++i) {
    if (!cache[i].valid) {
      victim = i;
      break;
    }
    if (cache[i].stamp < cache[victim].stamp) {
      victim = i;
    }
  }
  if (pattern.size() <= 4096) {
    cache[victim].compiled = CompiledLikePattern::Compile(pattern);
    cache[victim].stamp = ++clock;
    cache[victim].valid = true;
    return cache[victim].compiled.Matches(value);
  }
  return CompiledLikePattern::Compile(pattern).Matches(value);
}

// A borrowed, non-escaping byte LIKE pattern with a single literal and
// optional leading/trailing '%' (both may be present: Kind::kContains). The
// caller handles SQL NULL/collation and keeps the original pattern alive.
// Interior wildcards retain the general evaluator.
struct LiteralLikePattern {
  enum class Kind { kExact, kPrefix, kSuffix, kContains };
  Kind kind;
  std::string_view literal;

  static std::optional<LiteralLikePattern> Compile(std::string_view pattern) {
    if (pattern.find('\\') != std::string_view::npos) {
      return std::nullopt;
    }
    // Strip all consecutive wildcards at each end; %%abc%% is the same
    // pattern as %abc% and must still be treated as a pure substring match.
    const bool leading = pattern.starts_with('%');
    const bool trailing = pattern.ends_with('%');
    while (pattern.starts_with('%')) {
      pattern.remove_prefix(1);
    }
    while (pattern.ends_with('%')) {
      pattern.remove_suffix(1);
    }
    if (pattern.find_first_of("%_") != std::string_view::npos) {
      return std::nullopt;
    }
    return LiteralLikePattern{.kind = leading && trailing ? Kind::kContains
                                      : leading           ? Kind::kSuffix
                                      : trailing          ? Kind::kPrefix
                                                          : Kind::kExact,
                              .literal = pattern};
  }
  [[nodiscard]] bool Matches(std::string_view value) const noexcept {
    switch (kind) {
      case Kind::kExact:
        return value == literal;
      case Kind::kPrefix:
        return value.starts_with(literal);
      case Kind::kSuffix:
        return value.ends_with(literal);
      case Kind::kContains:
        return value.find(literal) != std::string_view::npos;
    }
    return false;
  }
};
}  // namespace tinylamb
#endif
