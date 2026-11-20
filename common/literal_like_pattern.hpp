/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_LITERAL_LIKE_PATTERN_HPP
#define TINYLAMB_COMMON_LITERAL_LIKE_PATTERN_HPP
#include <optional>
#include <string_view>

namespace tinylamb {
// A borrowed, non-escaping byte LIKE pattern with a single literal and
// optional leading/trailing '%' (both may be present: Kind::kContains). The caller handles SQL NULL/collation and keeps the
// original pattern alive. Interior wildcards retain the general evaluator.
struct LiteralLikePattern {
  enum class Kind { kExact, kPrefix, kSuffix, kContains };
  Kind kind;
  std::string_view literal;

  static std::optional<LiteralLikePattern> Compile(std::string_view pattern) {
    // Strip all consecutive wildcards at each end; %%abc%% is the same
    // pattern as %abc% and must still be treated as a pure substring match.
    const bool leading = pattern.starts_with('%');
    while (pattern.starts_with('%')) {
      pattern.remove_prefix(1);
    }
    const bool trailing = pattern.ends_with('%');
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
