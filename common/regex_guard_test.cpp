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

}  // namespace tinylamb
