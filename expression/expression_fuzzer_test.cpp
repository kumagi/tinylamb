/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Issue #33: differential AST-vs-bytecode/detail violations found by
// expression_fuzzer must have a committed replay/regression path, not just
// an abort + artifact in an ad-hoc findings dir (invariant 3).  This test
// replays every `*.test` file in TINYLAMB_EXPR_FUZZ_REGRESSION_DIR (raw
// libFuzzer inputs hex-encoded by WriteExpressionFuzzRepro) through the same
// ExpressionFuzzRun entry the fuzzer uses.

#include "expression/expression_fuzzer.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace tinylamb {
namespace {

std::vector<uint8_t> SampleBytes() {
  std::vector<uint8_t> bytes;
  bytes.reserve(160);
  for (int i = 0; i < 160; ++i) {
    bytes.push_back(static_cast<uint8_t>((i * 37) + 11));
  }
  return bytes;
}

TEST(ExpressionFuzzerReplayTest, ReproTextRoundTrips) {
  const std::vector<uint8_t> bytes = SampleBytes();
  const std::string text = ExpressionFuzzReproText(bytes.data(), bytes.size());
  const StatusOr<std::vector<uint8_t>> decoded =
      DecodeExpressionFuzzRepro(text);
  ASSERT_TRUE(decoded.HasValue());
  EXPECT_EQ(decoded.Value(), bytes);
}

TEST(ExpressionFuzzerReplayTest, DecoderRejectsMalformedInput) {
  EXPECT_FALSE(DecodeExpressionFuzzRepro("deadbee").HasValue());  // odd nibbles
  EXPECT_FALSE(DecodeExpressionFuzzRepro("deadbe z").HasValue());  // bad digit
  // Comments and whitespace are not payload: "#ff\ngg" carries no bytes but
  // 'g' is not hex either.
  EXPECT_FALSE(DecodeExpressionFuzzRepro("#ff\ngg").HasValue());
  const StatusOr<std::vector<uint8_t>> empty =
      DecodeExpressionFuzzRepro("# only comments\n");
  ASSERT_TRUE(empty.HasValue());
  EXPECT_TRUE(empty.Value().empty());
}

TEST(ExpressionFuzzerReplayTest, OutOfBandInputsAreSkipped) {
  // Same contract as the libFuzzer entry: too-small / too-large inputs are
  // ignored, they never count as violations.
  const std::vector<uint8_t> tiny = {1, 2, 3};
  const ExpressionFuzzResult tiny_result =
      ExpressionFuzzRun(tiny.data(), tiny.size());
  EXPECT_TRUE(tiny_result.ok);
  EXPECT_TRUE(tiny_result.ignored);
  const std::vector<uint8_t> huge(5000, 0);
  const ExpressionFuzzResult huge_result =
      ExpressionFuzzRun(huge.data(), huge.size());
  EXPECT_TRUE(huge_result.ok);
  EXPECT_TRUE(huge_result.ignored);
}

TEST(ExpressionFuzzerReplayTest, ReplayCommittedRegressionFiles) {
  const char* dir = std::getenv("TINYLAMB_EXPR_FUZZ_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "TINYLAMB_EXPR_FUZZ_REGRESSION_DIR not set";
  }
  std::error_code ec;
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    if (entry.is_regular_file() && entry.path().extension() == ".test") {
      files.push_back(entry.path());
    }
  }
  std::ranges::sort(files);
  // A wired-up-but-empty corpus would silently downgrade this guard to a
  // no-op; the committed findings must stay checked in.
  EXPECT_FALSE(files.empty()) << dir;
  for (const auto& path : files) {
    std::ifstream in(path);
    std::stringstream buffer;
    buffer << in.rdbuf();
    const StatusOr<std::vector<uint8_t>> decoded =
        DecodeExpressionFuzzRepro(buffer.str());
    ASSERT_TRUE(decoded.HasValue()) << path.string();
    const ExpressionFuzzResult result =
        ExpressionFuzzRun(decoded.Value().data(), decoded.Value().size());
    EXPECT_TRUE(result.ok) << path.string() << ": " << result.violation;
  }
}

}  // namespace
}  // namespace tinylamb
