/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Unit tests for the std::regex DoS guard (common/regex_guard.hpp): the
// nested-repetition analysis rejects unbounded quantifiers over ambiguous
// operands (variable-length elements, overlapping alternation branches,
// empty-matchable operands) while allowing bounded and deterministic shapes,
// and CompileCheckedRegex funnels rejected and malformed patterns into one
// StatusError channel.

#include "common/regex_guard.hpp"

#include <gtest/gtest.h>

#include <regex>
#include <string>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"

namespace tinylamb {

TEST(RegexGuardNestedRepetitionTest, RejectsVariableLengthOperands) {
  const std::vector<std::string> rejected = {
      "(a+)+",       // classic nested star
      ".++",         // quantified greedy dot
      "(a?)+",       // empty-matchable operand under '+'
      "a+{2,}*",     // stacked unbounded quantifiers
      "(\\d|\\w)+",  // overlapping one-letter escape classes
      "(x|xy)*",     // branch first-sets overlap
      "(a|a)+",      // identical branches
      "(a|)+",       // empty branch matches empty
      "((a|a)b)+",   // ambiguity propagated through a group
      "(?:a+)*",     // non-capturing group form
      "(a+){2,}",    // unbounded "{m,}" over a variable operand
      "(a+){0,}",    // unbounded "{m,}" lower bound zero
  };
  for (const std::string& pattern : rejected) {
    SCOPED_TRACE(pattern);
    EXPECT_TRUE(RegexPatternHasNestedRepetition(pattern)) << pattern;
  }
}

TEST(RegexGuardNestedRepetitionTest, AllowsDeterministicOrBoundedShapes) {
  const std::vector<std::string> allowed = {
      "abc",              // plain atoms
      "a+",               // deterministic unbounded operand
      "a*b*c*",           // sequential unbounded atoms
      "a.*b.*c",          // sequential dots
      "a+?",              // lazy modifier
      "a{2,4}",           // bounded repeat
      "(a+){3}",          // exact-count repeat of a variable operand
      "(cat|dog|bird)+",  // distinct fixed-length branches
      "(a|bc)*",          // disjoint fixed-length branches stay allowed
      "[0-9]+",           // character class
      "[^a-z]*",          // negated class
      "\\d{4}-\\d{2}",    // escapes with bounded repeat
      "a?b?",             // bounded optionals are fine sequentially
      "^start$",
      "(ab)*",  // fixed-length group under star
      "[[:digit:]]+",
  };
  for (const std::string& pattern : allowed) {
    SCOPED_TRACE(pattern);
    EXPECT_FALSE(RegexPatternHasNestedRepetition(pattern)) << pattern;
  }
}

TEST(RegexGuardCompileTest, CompilesHealthyPatterns) {
  auto compiled = CompileCheckedRegex("^[a-z]+_[0-9]{2}$");
  ASSERT_TRUE(compiled.HasValue());
  EXPECT_TRUE(std::regex_search("abc_12", compiled.MoveValue()));
  auto recompiled = CompileCheckedRegex("^[a-z]+_[0-9]{2}$");
  ASSERT_TRUE(recompiled.HasValue());
  EXPECT_FALSE(std::regex_search("12_abc", recompiled.MoveValue()));
}

TEST(RegexGuardCompileTest, RejectsAmbiguousPatternsWithOneError) {
  auto rejected = CompileCheckedRegex("(a+)+$");
  EXPECT_FALSE(rejected.HasValue());
  EXPECT_NE(rejected.GetStatus().GetMessage().find("nested repetition"),
            std::string::npos);
}

TEST(RegexGuardCompileTest, RejectsMalformedPatternsWithOneError) {
  auto malformed = CompileCheckedRegex("a([unclosed");
  EXPECT_FALSE(malformed.HasValue());
  EXPECT_NE(malformed.GetStatus().GetMessage().find("invalid regular"),
            std::string::npos);
}

TEST(RegexGuardCompileTest, CachedPatternsKeepIndependentPrograms) {
  for (int pass = 0; pass < 3; ++pass) {
    for (int i = 0; i < 20; ++i) {
      const std::string pattern = "prefix" + std::to_string(i) + "$";
      auto result = CompileCheckedRegex(pattern);
      ASSERT_TRUE(result.HasValue());
      const std::regex& compiled = result.Value();
      EXPECT_TRUE(std::regex_match("prefix" + std::to_string(i), compiled));
    }
  }
  EXPECT_FALSE(CompileCheckedRegex("(a+)+").HasValue());
}

TEST(RegexGuardCompileTest, ReplacementUsesGoogleSqlCaptures) {
  auto captured =
      ReplaceCheckedRegex("abc123def", "([a-z]+)([0-9]+)", R"(\2-\1-\0)");
  ASSERT_TRUE(captured.HasValue());
  EXPECT_EQ(captured.Value(), "123-abc-abc123def");
  auto followed_by_digit = ReplaceCheckedRegex("x", "(x)", R"(\19)");
  ASSERT_TRUE(followed_by_digit.HasValue());
  EXPECT_EQ(followed_by_digit.Value(), "x9");
  auto literal = ReplaceCheckedRegex("x", "(x)", R"($1&\\)");
  ASSERT_TRUE(literal.HasValue());
  EXPECT_EQ(literal.Value(), "$1&\\");
  EXPECT_FALSE(ReplaceCheckedRegex("x", "(x)", R"(\2)").HasValue());
  EXPECT_FALSE(ReplaceCheckedRegex("x", "(x)", "\\").HasValue());
}

TEST(RegexGuardCompileTest, ReplacementMatchesReferenceForByteAndEmptyMatches) {
  const std::vector<std::string> patterns{
      "a*",      "(a|b)+", "[^/]+", "^.*$",        R"(\s+)", R"([\s\S]*)",
      R"(a\.b)", "^a$",    "a|",    "[[:space:]]", "(a)?b"};
  const std::vector<std::string> inputs{
      "", "a", "ab", "a.b", "aaab", "a\nb", "a\rb", "a\vb", "a\n", "日本語"};
  for (const auto& pattern : patterns) {
    for (const auto& input : inputs) {
      SCOPED_TRACE(pattern);
      SCOPED_TRACE(input);
      auto result = ReplaceCheckedRegex(input, pattern, "replacement");
      ASSERT_TRUE(result.HasValue());
      EXPECT_EQ(result.Value(),
                std::regex_replace(input, std::regex(pattern), "replacement"));
    }
  }
}

TEST(RegexGuardCompileTest, ReplacementViewsRespectBoundsAndCacheOwnership) {
  std::string input("p:a\0b:s", 7);
  std::string pattern = "!(a)?";
  std::string replacement = R"(!\1$?)";
  const auto run = [&] {
    return ReplaceCheckedRegex(std::string_view(input).substr(2, 3),
                               std::string_view(pattern).substr(1, 3),
                               std::string_view(replacement).substr(1, 3));
  };
  for (int pass = 0; pass < 2; ++pass) {
    auto result = run();
    ASSERT_TRUE(result.HasValue());
    EXPECT_EQ(result.Value(), std::string("a$\0b", 4));
  }
  // All cache keys must own their bytes after the input views expire or change.
  pattern = "!(b)?";
  replacement = R"(!$\1?)";
  auto changed = run();
  ASSERT_TRUE(changed.HasValue());
  EXPECT_EQ(changed.Value(), std::string("a\0$b", 4));
  auto previous = ReplaceCheckedRegex("a", "(a)", R"(\1$)");
  ASSERT_TRUE(previous.HasValue());
  EXPECT_EQ(previous.Value(), "a$");
  auto empty = ReplaceCheckedRegex({}, {}, "x");
  ASSERT_TRUE(empty.HasValue());
  EXPECT_EQ(empty.Value(), "x");
  auto nul_pattern = ReplaceCheckedRegex(std::string_view(input).substr(2, 3),
                                         std::string_view("\0", 1), "_");
  ASSERT_TRUE(nul_pattern.HasValue());
  EXPECT_EQ(nul_pattern.Value(), "a_b");
}

TEST(RegexGuardCompileTest, ReplacementProgramsKeepKeysAndUncachedLimits) {
  for (int pass = 0; pass < 3; ++pass) {
    for (int i = 0; i < 20; ++i) {
      const std::string suffix = std::to_string(i);
      auto result = ReplaceCheckedRegex("v", "(v)", R"(\1)" + suffix);
      ASSERT_TRUE(result.HasValue());
      EXPECT_EQ(result.Value(), "v" + suffix);
    }
    auto two = ReplaceCheckedRegex("vw", "(v)(w)", R"(\2)");
    ASSERT_TRUE(two.HasValue());
    EXPECT_EQ(two.Value(), "w");
    EXPECT_FALSE(ReplaceCheckedRegex("v", "(v)", R"(\2)").HasValue());
    EXPECT_FALSE(ReplaceCheckedRegex("v", "(v)", R"(\a)").HasValue());
    const auto rejected = ReplaceCheckedRegex("v", "(v+)+", R"(\a)");
    EXPECT_NE(rejected.GetStatus().GetMessage().find("nested repetition"),
              std::string::npos);
    const std::string large_replacement = std::string(4097, 'x') + "$";
    auto large = ReplaceCheckedRegex("v", "(v)", large_replacement);
    ASSERT_TRUE(large.HasValue());
    EXPECT_EQ(large.Value(), large_replacement);
    // One large character class exercises an uncached pattern without deep
    // recursive regex syntax. Neither size limit may change the result.
    auto long_pattern =
        ReplaceCheckedRegex("a", "[" + std::string(4096, 'a') + "]", "z");
    ASSERT_TRUE(long_pattern.HasValue());
    EXPECT_EQ(long_pattern.Value(), "z");
  }
}

}  // namespace tinylamb
