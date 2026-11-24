/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Unit tests for the std::regex DoS guard (common/regex_guard.hpp): the
// nested-repetition analysis rejects unbounded quantifiers over ambiguous
// operands (variable-length elements, overlapping alternation branches,
// empty-matchable operands) while allowing bounded and deterministic shapes,
// and CompileCheckedRegex funnels rejected and malformed patterns into one
// StatusError channel.

#include "common/regex_guard.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
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
      "(a|bc)*",          // different-length branches with disjoint first sets
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

TEST(RegexGuardCompileTest, Utf8DotSkipsBothLineTerminators) {
  for (bool bytes : {false, true}) {
    EXPECT_EQ(CheckedRegexPosition("\r\nX", ".", 1, 1, 0, bytes).Value(), 3);
  }
  EXPECT_EQ(CheckedRegexPosition("猫", "^.$", 1, 1, 0, false).Value(), 1);
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

TEST(RegexGuardCompileTest, OneCacheMissPreservesOtherPrograms) {
  static thread_local int compiles = 0;
  static std::atomic<std::uint64_t> invocation{0};
  const std::uint64_t run = invocation.fetch_add(1, std::memory_order_relaxed);
  constexpr auto transform = [](const std::string& pattern) {
    ++compiles;
    return pattern;
  };
  const int before = compiles;
  for (int i = 0; i < 9; ++i) {
    ASSERT_TRUE(
        regex_guard_detail::CompileCheckedPattern<transform>(
            "cache-capacity-" + std::to_string(run) + "-" + std::to_string(i))
            .HasValue());
  }
  ASSERT_EQ(compiles, before + 9);
  ASSERT_TRUE(regex_guard_detail::CompileCheckedPattern<transform>(
                  "cache-capacity-" + std::to_string(run) + "-1")
                  .HasValue());
  EXPECT_EQ(compiles, before + 9);
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

TEST(RegexGuardCompileTest, ZeroCopyViewMatchesReferenceAcrossEdgeCases) {
  const std::string url_pattern = R"(^https?://(?:www\.)?([^/]+)/[\s\S]*$)";
  const std::vector<std::string> urls{
      "",
      "https://www.google.com/search?q=tinylamb&hl=ja",
      "http://yandex.ru:8080/path/to/page\nwith\nnewlines",
      "http://www./backtrack-into-host",
      "https://www.www.example.org/a/b/c",
      "https://no-trailing-slash.example.com",
      "ftp://www.example.com/file",
      std::string("http://host\0with\0nul/tail", 26),
  };
  std::string scratch = "sentinel";
  for (const std::string& url : urls) {
    auto expected = ReplaceCheckedRegex(url, url_pattern, R"(\1)");
    ASSERT_TRUE(expected.HasValue());
    const std::string ref =
        std::regex_replace(url, std::regex(url_pattern), "$01");
    EXPECT_EQ(expected.Value(), ref);

    scratch = "untouched";
    const std::string_view input_view(url);
    auto actual =
        ReplaceCheckedRegexView(input_view, url_pattern, R"(\1)", &scratch);
    ASSERT_TRUE(actual.HasValue());
    EXPECT_EQ(actual.Value(), ref);
    if (!input_view.empty()) {
      // Single-capture or non-match must point directly into `input_view`
      // without touching `scratch`.
      EXPECT_GE(actual.Value().data(), input_view.data());
      EXPECT_LE(actual.Value().data() + actual.Value().size(),
                input_view.data() + input_view.size());
      EXPECT_EQ(scratch, "untouched");
    }
  }

  // Composite replacement on the same anchored pattern uses `scratch` cleanly.
  const std::string sample = "https://www.example.com/index.html";
  auto formatted =
      ReplaceCheckedRegexView(sample, url_pattern, R"(<\1:\0>)", &scratch);
  ASSERT_TRUE(formatted.HasValue());
  EXPECT_EQ(formatted.Value(), "<example.com:" + sample + ">");

  // Disjoint vs overlapping character-class repetitions must both match
  // std::regex (overlapping falls back rather than mis-parsing).
  const std::vector<std::string> tricky_patterns{
      R"(^([a-z]+)([0-9]+)$)",
      R"(^([a-z]+)(?:foo)?$)",
      R"(^(ab|a)[\s\S]*$)",
      R"(^(a|ab)[\s\S]*$)",
      R"(^([^:]+):(?:\/\/)?([^/]+)$)",
  };
  const std::vector<std::string> tricky_inputs{
      "", "abc123", "abcfoo", "abc", "ab", "a", "http://host", "http:host"};
  for (const std::string& pat : tricky_patterns) {
    for (const std::string& in : tricky_inputs) {
      auto view_res = ReplaceCheckedRegexView(in, pat, R"(\1)", &scratch);
      ASSERT_TRUE(view_res.HasValue());
      EXPECT_EQ(std::string(view_res.Value()),
                std::regex_replace(in, std::regex(pat), "$01"))
          << "pattern=" << pat << " input=" << in;
    }
  }
}

// P464: replacement templates over 4096 bytes must still be cached -- the
// old gate re-ran the full CompileReplacement() (nested-repetition scan +
// std::regex + RE2) for every row of a stable large template.
TEST(RegexGuardTest, LargeReplacementTemplateCachedUnderBudget) {
  const std::string pattern = "^x";
  const std::string big(5000, 'r');
  const size_t before = ReplacementProgramFullCompilesForTest();
  for (int call = 0; call < 8; ++call) {
    StatusOr<std::string> out = ReplaceCheckedRegex("xabc", pattern, big);
    ASSERT_TRUE(out.HasValue());
    EXPECT_EQ(out.Value(), big + "abc");
  }
  EXPECT_EQ(ReplacementProgramFullCompilesForTest(), before + 1)
      << "stable >4096-byte template must compile exactly once";

  // Distinct large templates beyond the entry budget stay correct; the
  // budgeted eviction just re-compiles them (one compile each).
  for (int i = 0; i < 12; ++i) {
    const std::string rep = std::string(500, static_cast<char>('a' + i)) + big;
    StatusOr<std::string> out = ReplaceCheckedRegex("xabc", pattern, rep);
    ASSERT_TRUE(out.HasValue());
    EXPECT_EQ(out.Value(), rep + "abc");
    EXPECT_EQ(ReplacementProgramFullCompilesForTest(),
              before + 2 + static_cast<size_t>(i));
  }
}

TEST(RegexGuardTest, CheckedRegexContainsMatchesReferenceAcrossPatterns) {
  struct FastRegexGuard {  // NOLINT(cppcoreguidelines-special-member-functions):
                           // scope-owned cleanup.
    ~FastRegexGuard() { SetFastRegexDisabledForTest(false); }
  } guard;

  const std::vector<std::string> patterns{
      "",
      "a",
      "checkout-[0-9]+-vip",
      "(ab|cd)[0-9]+(xy|zw)",
      "(ab|b)c",  // ThompsonSearch start-injection-at-every-pos trap on "xbc"
      "(abc)+",
      "abc+",
      "a*",
      "a+b?",
      "foo|bar",
      "(foo.*1|foo.*2)",
      "[0-9]{2}-[a-z]{3}",
      "[^0-9]+",
      R"(\d+\s+\w+)",
      "a.b",
      R"(^prefix_[0-9]+$)",
      "(?:ab|cd)*ef",
  };
  const std::vector<std::string> inputs{
      "",
      "a",
      "b",
      "xbc",
      "ab",
      "abc",
      "abcc",
      "abcabc",
      "zzzcheckout-42-vip_ok",
      "checkout-vip",
      "xxcd99zw_tail",
      "xxcdzw",
      "12-abc",
      "1-abc",
      "123\tword_9",
      "a\nb",
      "a\rb",
      "aXb",
      "prefix_123",
      "prefix_123_extra",
      "foo_mid_2",
      "ef",
      "\xE6\x97\xA5"
      "abc\xE6\x9C\xAC",
      std::string("p:a\0b:q", 7),
  };

  for (const std::string& pat : patterns) {
    const std::regex re(pat);
    for (const std::string& in : inputs) {
      SCOPED_TRACE(pat);
      SCOPED_TRACE(in);
      const bool expected = std::regex_search(in.begin(), in.end(), re);
      SetFastRegexDisabledForTest(true);
      auto off = CheckedRegexContains(in, pat);
      ASSERT_TRUE(off.HasValue());
      EXPECT_EQ(off.Value(), expected);
      SetFastRegexDisabledForTest(false);
      auto on = CheckedRegexContains(in, pat);
      ASSERT_TRUE(on.HasValue());
      EXPECT_EQ(on.Value(), expected);
    }
  }

  // Invalid and nested-repetition patterns must still error cleanly.
  EXPECT_FALSE(CheckedRegexContains("aaa", "(a+)+").HasValue());
  EXPECT_FALSE(CheckedRegexContains("abc", "([unclosed").HasValue());
}

}  // namespace tinylamb
