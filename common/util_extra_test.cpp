/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Extra coverage for the self-contained digests in common/digest.hpp (RFC
// vectors, block-boundary inputs, incremental == one-shot) and ToHex.
#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "common/digest.hpp"

namespace tinylamb {

namespace {

// Classic RFC 1321 / 3174 / 4634 vectors plus a block-boundary input that
// makes the streaming buffer hand off exactly one full block.
constexpr std::string_view kInputs[] = {
    "", "abc", "The quick brown fox jumps over the lazy dog",
    "0123456789012345678901234567890123456789012345678901234567890123"};

std::string OneShot(std::string_view input, int which) {
  switch (which) {
    case 0:
      return digest::ToHex(digest::Md5Digest(input));
    case 1:
      return digest::ToHex(digest::Sha1Digest(input));
    case 2:
      return digest::ToHex(digest::Sha256Digest(input));
    default:
      return digest::ToHex(digest::Sha512Digest(input));
  }
}

template <class Hash>
std::string IncrementalHex(std::string_view input, size_t step) {
  Hash hash;
  for (size_t i = 0; i < input.size(); i += step) {
    hash.Update(input.substr(i, step));
  }
  return digest::ToHex(hash.Finalize());
}

}  // namespace

TEST(DigestExtraTest, Md5MatchesRfc1321Vectors) {
  EXPECT_EQ(OneShot("", 0), "d41d8cd98f00b204e9800998ecf8427e");
  EXPECT_EQ(OneShot("abc", 0), "900150983cd24fb0d6963f7d28e17f72");
}

TEST(DigestExtraTest, Sha1MatchesKnownVectors) {
  EXPECT_EQ(OneShot("", 1), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
  EXPECT_EQ(OneShot("abc", 1), "a9993e364706816aba3e25717850c26c9cd0d89d");
}

TEST(DigestExtraTest, Sha256MatchesKnownVectors) {
  EXPECT_EQ(OneShot("", 2),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(OneShot("abc", 2),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(DigestExtraTest, Sha512MatchesKnownVectors) {
  EXPECT_EQ(OneShot("", 3),
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce"
            "47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");
  EXPECT_EQ(OneShot("abc", 3),
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a"
            "2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f");
}

TEST(DigestExtraTest, IncrementalUpdatesEqualOneShotAcrossBlockBoundaries) {
  const std::string long_input(1000, 'a');
  for (const size_t step : {size_t{1}, size_t{63}, size_t{64}, size_t{65},
                            size_t{127}, size_t{333}}) {
    EXPECT_EQ((IncrementalHex<digest::Md5>(long_input, step)),
              (IncrementalHex<digest::Md5>(long_input, long_input.size())))
        << "step=" << step;
    EXPECT_EQ((IncrementalHex<digest::Sha1>(long_input, step)),
              (IncrementalHex<digest::Sha1>(long_input, long_input.size())))
        << "step=" << step;
    EXPECT_EQ((IncrementalHex<digest::Sha256>(long_input, step)),
              (IncrementalHex<digest::Sha256>(long_input, long_input.size())))
        << "step=" << step;
    EXPECT_EQ((IncrementalHex<digest::Sha512>(long_input, step)),
              (IncrementalHex<digest::Sha512>(long_input, long_input.size())))
        << "step=" << step;
  }
}

TEST(DigestExtraTest, DistinctInputsNeverShareADigest) {
  for (int which = 0; which < 4; ++which) {
    EXPECT_NE(OneShot(kInputs[0], which), OneShot(kInputs[1], which));
    EXPECT_NE(OneShot(kInputs[1], which), OneShot(kInputs[2], which));
    EXPECT_NE(OneShot(kInputs[2], which), OneShot(kInputs[3], which));
  }
}

TEST(DigestExtraTest, ToHexEncodesNibblesAndSurvivesRoundTripSize) {
  EXPECT_EQ(digest::ToHex(""), "");
  EXPECT_EQ(digest::ToHex(std::string({'\x00', '\x0f', '\xf0', '\xff'})),
            "000ff0ff");
  const std::string raw = digest::Md5Digest("abc");
  EXPECT_EQ(raw.size(), 16U);
  EXPECT_EQ(digest::ToHex(raw).size(), 32U);
}

}  // namespace tinylamb
