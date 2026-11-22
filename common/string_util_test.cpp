/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/string_util.hpp"

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "common/literal_like_pattern.hpp"
#include "gtest/gtest.h"

namespace {

// #1056 performance pins use a global operator new replacement; only the
// measuring thread counts, so sanitizer background allocations cannot flip
// the assertion (same idiom as common/decoder_test.cpp).
std::atomic<uint64_t> g_alloc_count{0};
std::atomic<bool> g_counting{false};
std::atomic<std::thread::id> g_measuring_thread;

inline void CountAllocationIfActive() {
  if (g_counting.load(std::memory_order_relaxed) &&
      std::this_thread::get_id() ==
          g_measuring_thread.load(std::memory_order_relaxed)) {
    g_alloc_count.fetch_add(1, std::memory_order_relaxed);
  }
}

}  // namespace

void* operator new(std::size_t n) {
  CountAllocationIfActive();
  // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
  // intentional in this code
  void* p = std::malloc(n == 0 ? 1 : n);
  // NOLINTNEXTLINE(cppcoreguidelines-no-malloc) raw-buffer test
  if (p == nullptr) {
    throw std::bad_alloc();
  }
  return p;
}
void* operator new[](std::size_t n) { return operator new(n); }
void* operator new(std::size_t n, std::align_val_t a) {
  CountAllocationIfActive();
  const auto align = static_cast<std::size_t>(a);
  std::size_t size = n == 0 ? 1 : n;
  if (!std::has_single_bit(align) ||
      size > std::numeric_limits<std::size_t>::max() - (align - 1)) {
    throw std::bad_alloc();
  }
  size = (size + align - 1) & ~(align - 1);
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)  // raw-buffer codec test
  void* p = std::aligned_alloc(align, size);
  if (p == nullptr) {
    throw std::bad_alloc();
  }
  return p;
}
void* operator new[](std::size_t n, std::align_val_t a) {
  return operator new(n, a);
}
// NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
// intentional in this code
void operator delete(void* p) noexcept { std::free(p); }
// NOLINTNEXTLINE(cppcoreguidelines-no-malloc) raw-buffer test
// NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
// intentional in this code
void operator delete[](void* p) noexcept { std::free(p); }
// NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
// intentional in this code
void operator delete(void* p, std::size_t /*unused*/) noexcept { std::free(p); }
// NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
// intentional in this code
void operator delete[](void* p, std::size_t /*unused*/) noexcept {
  std::free(p);
}
// NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
// intentional in this code
void operator delete(void* p, std::align_val_t /*unused*/) noexcept {
  std::free(p);
}
// NOLINTNEXTLINE(cppcoreguidelines-no-malloc) raw-buffer test
// NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
// intentional in this code
void operator delete[](void* p, std::align_val_t /*unused*/) noexcept {
  std::free(p);
}
// NOLINTNEXTLINE(cppcoreguidelines-no-malloc) raw-buffer test
void operator delete(void* p, std::size_t /*unused*/,
                     std::align_val_t /*unused*/) noexcept {
  // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
  // intentional in this code
  std::free(p);
}
// NOLINTNEXTLINE(cppcoreguidelines-no-malloc) raw-buffer test
void operator delete[](void* p, std::size_t /*unused*/,
                       std::align_val_t /*unused*/) noexcept {
  // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
  // intentional in this code
  std::free(p);
}

namespace tinylamb {
namespace {

TEST(StringUtilTest,
     LiteralLikePatternHandlesEdgesAndDeclinesGeneralWildcards) {
  // NOLINTNEXTLINE(cppcoreguidelines-no-malloc) raw-buffer test
  for (const auto* const pattern :
       {"", "%", "%%", "abc", "abc%", "%abc", "%abc%"}) {
    const auto compiled = LiteralLikePattern::Compile(pattern);
    if (!compiled.has_value()) {
      ADD_FAILURE() << "Expected pattern to compile: " << pattern;
      continue;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-no-malloc) raw-buffer test
    for (const auto* const value :
         {"", "abc", "xabc", "abcx", "xabcx", "ABC"}) {
      const std::string_view p(pattern), v(value);
      const bool expected =
          p == "%" || p == "%%" || (p.empty() && v.empty()) ||
          (p == "abc" && v == "abc") || (p == "abc%" && v.starts_with("abc")) ||
          (p == "%abc" && v.ends_with("abc")) ||
          (p == "%abc%" && v.find("abc") != std::string_view::npos);
      EXPECT_EQ(compiled->Matches(value), expected)
          << pattern << " / " << value;
    }
  }
  for (const auto* const pattern : {"a_c", "a%b", "%a%b%"}) {
    EXPECT_FALSE(LiteralLikePattern::Compile(pattern));
  }
  // Runs of '%' at the ends collapse, so "%%%" is the match-everything
  // pattern rather than an interior wildcard.
  const auto all = LiteralLikePattern::Compile("%%%");
  ASSERT_TRUE(all.has_value());
  for (const auto* const value : {"", "abc", "%"}) {
    EXPECT_TRUE(all->Matches(value)) << value;
  }
}

TEST(StringUtilTest, EscapedLikeWildcardsRemainLiteral) {
  EXPECT_FALSE(LiteralLikePattern::Compile("abc\\%"));
  EXPECT_TRUE(EscapedLikeMatch("abc%", "abc\\%"));
  EXPECT_FALSE(EscapedLikeMatch("abcxyz", "abc\\%"));
  EXPECT_TRUE(EscapedLikeMatch("x_a", "%\\_a"));
  EXPECT_FALSE(EscapedLikeMatch("xxa", "%\\_a"));
  EXPECT_TRUE(EscapedLikeMatch("é%", "_\\%"));
  EXPECT_TRUE(EscapedLikeMatch("a\\tail", "a\\\\%"));
}

TEST(StringUtilTest, AsciiCaseConversion) {
  EXPECT_EQ(AsciiToLower('A'), 'a');
  EXPECT_EQ(AsciiToLower('Z'), 'z');
  EXPECT_EQ(AsciiToLower('a'), 'a');
  EXPECT_EQ(AsciiToLower('5'), '5');
  EXPECT_EQ(AsciiToUpper('a'), 'A');
  EXPECT_EQ(AsciiToUpper('z'), 'Z');
  EXPECT_EQ(AsciiToUpper('A'), 'A');
  EXPECT_EQ(AsciiToUpper('_'), '_');

  EXPECT_EQ(AsciiToLower("Select_123_From"), "select_123_from");
  EXPECT_EQ(AsciiToUpper("Select_123_From"), "SELECT_123_FROM");
  EXPECT_EQ(AsciiToLower(""), "");
  EXPECT_EQ(AsciiToUpper(""), "");
}

TEST(StringUtilTest, IdentifierEquals) {
  EXPECT_TRUE(IdentifierEquals("", ""));
  EXPECT_TRUE(IdentifierEquals("l_orderkey", "l_orderkey"));
  EXPECT_TRUE(IdentifierEquals("L_ORDERKEY", "l_orderkey"));
  EXPECT_TRUE(IdentifierEquals("Customer_ID", "customer_id"));
  EXPECT_FALSE(IdentifierEquals("l_orderkey", "o_orderkey"));
  EXPECT_FALSE(IdentifierEquals("col", "col1"));
  EXPECT_FALSE(IdentifierEquals("a_b", "a-b"));
}

TEST(StringUtilTest, ContainsIdentifier) {
  const std::vector<std::string> cols = {"L_OrderKey", "o_custkey", "c_name"};
  EXPECT_TRUE(ContainsIdentifier(cols, "l_orderkey"));
  EXPECT_TRUE(ContainsIdentifier(cols, "O_CUSTKEY"));
  EXPECT_TRUE(ContainsIdentifier(cols, "C_Name"));
  EXPECT_FALSE(ContainsIdentifier(cols, "p_partkey"));
  EXPECT_FALSE(ContainsIdentifier({}, "l_orderkey"));
}

TEST(StringUtilTest, TrimAsciiWhitespace) {
  EXPECT_EQ(TrimAsciiWhitespace("  hello \t\n\r "), "hello");
  EXPECT_EQ(TrimAsciiWhitespace("no_spaces"), "no_spaces");
  EXPECT_EQ(TrimAsciiWhitespace("   \t\n\r"), "");
  EXPECT_EQ(TrimAsciiWhitespace(""), "");
}

// #1056 pins for the bitmap/inline-set rewrite of TrimCodePoints: membership
// structure changed, semantics must not.
TEST(StringUtilTest, TrimCodePointsAsciiCutset) {
  EXPECT_EQ(TrimCodePoints(" \t x \n", " \t\n\r\f\v", true, true), "x");
  EXPECT_EQ(TrimCodePoints("  ab  ", " ", true, true), "ab");
  EXPECT_EQ(TrimCodePoints("", " ", true, true), "");
  EXPECT_EQ(TrimCodePoints("ab", "", true, true), "ab");
  EXPECT_EQ(TrimCodePoints("  ab  ", " ", false, true), "  ab");
  EXPECT_EQ(TrimCodePoints("  ab  ", " ", true, false), "ab  ");
}

TEST(StringUtilTest, TrimCodePointsMultiByteCutset) {
  // 'Á' is one code point (C3 81); it must never cut the lead byte of an
  // unrelated character sharing that lead byte.
  EXPECT_EQ(TrimCodePoints("\xC3\x81x\xC3\x81", "\xC3\x81", true, true), "x");
  EXPECT_EQ(TrimCodePoints("\xC3\x83x", "\xC3\x81", true, true),
            "\xC3\x83x");  // 'Ã' != 'Á'
  // A lone continuation byte from a BYTES cutset keeps byte semantics.
  EXPECT_EQ(TrimCodePoints("\xACx", "\xAC", true, true), "x");
  // Leading byte of a well-formed sequence stripped by a one-byte (invalid
  // lead) cutset point.
  EXPECT_EQ(TrimCodePoints("\xC3\xA9x", "\xC3", true, true), "\xA9x");
  // More multi-byte points than the inline array (8) exercises the overflow
  // bucket.
  std::string big_cutset;
  big_cutset.reserve(20);
  std::string big_text;
  big_text.reserve(20);
  for (int i = 0; i < 10; ++i) {
    big_cutset.append("\xC3", 1);
    big_cutset.push_back(static_cast<char>(0x81 + i));
    big_text.append("\xC3", 1);
    big_text.push_back(static_cast<char>(0x81 + i));
  }
  big_text += "x";
  big_text += big_cutset;
  EXPECT_EQ(TrimCodePoints(big_text, big_cutset, true, true), "x");
}

TEST(StringUtilTest, TrimCodePointsDoesNotAllocateForTypicalCutsets) {
  const std::string text = "  hello world, this string has some padding   ";
  const std::string_view ascii_cutset = " \t\n\r\f\v";
  // Warm up any lazy static init outside the window.
  constexpr std::string_view trimmed =
      "hello world, this string has some padding";
  EXPECT_EQ(TrimCodePoints(text, ascii_cutset, true, true), trimmed);
  std::atomic<uint64_t> sink{0};
  const auto measure = [&](std::string_view cutset) {
    g_alloc_count.store(0, std::memory_order_relaxed);
    g_measuring_thread.store(std::this_thread::get_id(),
                             std::memory_order_relaxed);
    struct StopCountingOnExit {
      ~StopCountingOnExit() {
        g_counting.store(false, std::memory_order_seq_cst);
      }
    };
    g_counting.store(true, std::memory_order_seq_cst);
    const StopCountingOnExit counting;
    for (int i = 0; i < 100; ++i) {
      sink.fetch_add(TrimCodePoints(text, cutset, true, true).size(),
                     std::memory_order_relaxed);
    }
    return g_alloc_count.load(std::memory_order_relaxed);
  };
  // The old body allocated the per-call vector<string_view> on every row.
  EXPECT_EQ(measure(ascii_cutset), 0U);
  const std::string multi = "\xC3\x81\xC3\x82\xC3\x83";  // 3 inline points
  EXPECT_EQ(measure(multi), 0U);
  EXPECT_EQ(sink.load(), 100U * (text.size() + trimmed.size()));
}

// T1542: overlong leads (0xC0/0xC1) and above-U+10FFFF leads (0xF5..0xF7)
// start sequences under the old masking, so TRIM/LIKE consumed malformed
// bytes as one character while the canonical validator (IsValidUtf8Text)
// accepts only C2..DF / E0..EF / F0..F4 leads.  Same window now.
TEST(StringUtilTest, Utf8ValidatorAcceptsLastTwoByteLead) {
  EXPECT_TRUE(IsValidUtf8Text("\xDF\x80"));
  EXPECT_TRUE(IsValidUtf8Text("\xDF\xBF"));
  EXPECT_FALSE(IsValidUtf8Text("\xDF"));
  EXPECT_FALSE(IsValidUtf8Text("\xDF\xC0"));
}

TEST(StringUtilTest, Utf8SequenceLengthMatchesValidatorWindow) {
  EXPECT_EQ(Utf8SequenceLength(0x00), 1U);
  EXPECT_EQ(Utf8SequenceLength(0x7F), 1U);
  EXPECT_EQ(Utf8SequenceLength(0x80), 1U);  // stray continuation byte
  EXPECT_EQ(Utf8SequenceLength(0xBF), 1U);
  EXPECT_EQ(Utf8SequenceLength(0xC0), 1U);  // overlong 2-byte lead
  EXPECT_EQ(Utf8SequenceLength(0xC1), 1U);  // overlong 2-byte lead
  EXPECT_EQ(Utf8SequenceLength(0xC2), 2U);
  EXPECT_EQ(Utf8SequenceLength(0xDF), 2U);
  EXPECT_EQ(Utf8SequenceLength(0xE0), 3U);
  EXPECT_EQ(Utf8SequenceLength(0xEF), 3U);
  EXPECT_EQ(Utf8SequenceLength(0xF0), 4U);
  EXPECT_EQ(Utf8SequenceLength(0xF4), 4U);
  EXPECT_EQ(Utf8SequenceLength(0xF5), 1U);  // lead beyond U+10FFFF
  EXPECT_EQ(Utf8SequenceLength(0xF7), 1U);
  EXPECT_EQ(Utf8SequenceLength(0xF8), 1U);
  EXPECT_EQ(Utf8SequenceLength(0xFF), 1U);

  // Consequence pin: a malformed lead degrades to one byte, so trimming it
  // removes only itself -- previously it swallowed the following 'x'.
  EXPECT_EQ(
      TrimCodePoints(std::string_view("\xC0x", 2), std::string_view("\xC0", 1),
                     /*leading=*/true, /*trailing=*/false),
      "x");
}

}  // namespace
}  // namespace tinylamb
