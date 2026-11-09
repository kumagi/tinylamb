/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_join_fuzzer.hpp"

#include <dirent.h>

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace tinylamb {

namespace {

std::vector<std::string> ListTestFiles(const std::string& dir) {
  std::vector<std::string> files;
  DIR* handle = opendir(dir.c_str());
  if (handle == nullptr) {
    return files;
  }
  while (const dirent* entry = readdir(handle)) {
    const std::string name = entry->d_name;
    if (name.size() > 5 && name.substr(name.size() - 5) == ".test") {
      std::string path = dir;
      path += "/";
      path += name;
      files.push_back(path);
    }
  }
  closedir(handle);
  return files;
}

}  // namespace

TEST(SqlJoinFuzzer, SeededJoinsMatchMirror) {
  int queries = 0;
  int left_joins = 0;
  const char* scale = std::getenv("TINYLAMB_FUZZ_ITERS");
  const int kIterations =
      (scale != nullptr) ? static_cast<int>(std::strtol(scale, nullptr, 10))
                         : 48;
  for (uint32_t seed = 0; std::cmp_less(seed, kIterations); ++seed) {
    std::mt19937 rng(seed);
    JoinStats stats;
    std::string report = RunJoinIteration(rng, false, &stats);
    ASSERT_EQ(report, "") << "failing seed=" << seed << "\n" << report;
    queries += stats.queries;
    left_joins += stats.left_joins;
  }
  EXPECT_GT(queries, kIterations) << "join oracle almost never ran";
  EXPECT_GT(left_joins, 0) << "LEFT JOIN never exercised";
}

TEST(SqlJoinFuzzer, TestFileRoundTripDetectsMismatch) {
  JoinTrace trace;
  trace.setup = {
      "CREATE TABLE fact (u INT64, k INT64, a INT64);",
      "CREATE TABLE dim (k INT64, tag VARCHAR(8));",
      "INSERT INTO fact VALUES (0, 1, 10);",
      "INSERT INTO dim VALUES (1, 'x');",
  };
  trace.queries = {
      {"SELECT f.u FROM fact f JOIN dim d ON f.k = d.k;", {"|0|,"}},
      {"SELECT f.u, d.tag FROM fact f LEFT JOIN dim d ON f.k = d.k;",
       {"|0|,|\"x\"|,"}},
  };
  const std::string text =
      SerializeJoinTest(3, trace, "synthetic for pipeline test");
  uint64_t seed = 0;
  JoinTrace parsed;
  std::string summary;
  ASSERT_TRUE(ParseJoinTest(text, &seed, &parsed, &summary)) << text;
  EXPECT_EQ(seed, 3U);
  EXPECT_EQ(parsed, trace);
  EXPECT_EQ(ReplayJoinTrace(parsed), "") << "honest trace mismatched";

  JoinTrace broken = trace;
  broken.queries[0].expected = {"|7|,"};
  const std::string broken_text = SerializeJoinTest(4, broken, "");
  uint64_t seed2 = 0;
  JoinTrace parsed2;
  std::string summary2;
  ASSERT_TRUE(ParseJoinTest(broken_text, &seed2, &parsed2, &summary2));
  EXPECT_NE(ReplayJoinTrace(parsed2), "") << "tampered trace replayed clean";
}

TEST(SqlJoinFuzzer, SixAggregateFusedJoinAndCrashSeedsRegression) {
  JoinTrace trace;
  trace.setup = {
      "CREATE TABLE fact (u INT64, k INT64, a INT64);",
      "CREATE TABLE dim (k INT64, tag VARCHAR(8));",
      "INSERT INTO fact VALUES (0, NULL, NULL);",
      "INSERT INTO fact VALUES (1, 2, -2);",
      "INSERT INTO fact VALUES (2, 1, 0);",
      "INSERT INTO fact VALUES (3, 2, -1);",
      "INSERT INTO fact VALUES (4, 1, 3);",
      "INSERT INTO dim VALUES (1, 'yy');",
      "INSERT INTO dim VALUES (3, 'x');",
      "INSERT INTO dim VALUES (1, 'x');",
      "INSERT INTO dim VALUES (NULL, 'x');",
      "INSERT INTO dim VALUES (1, 'yy');",
  };
  trace.queries = {
      {"SELECT COUNT(*), COUNT(d.tag), COUNT(DISTINCT f.k), SUM(f.a), MIN(f.a),"
       " MAX(d.k) FROM fact f LEFT JOIN dim d ON f.k = d.k;",
       {"|9|,|6|,|2|,|6|,|-2|,|1|,"}},
      {"SELECT COUNT(*), COUNT(d.tag), COUNT(DISTINCT f.k), SUM(f.a), MIN(f.a),"
       " MAX(d.k) FROM fact f JOIN dim d USING (k);",
       {"|6|,|6|,|1|,|9|,|0|,|1|,"}},
      {"SELECT COUNT(*), COUNT(d.tag), COUNT(DISTINCT f.k), SUM(f.a), MIN(f.a),"
       " MAX(d.k) FROM fact f CROSS JOIN dim d;",
       {"|25|,|25|,|2|,|0|,|-2|,|3|,"}},
  };
  EXPECT_EQ(ReplayJoinTrace(trace), "");

  static constexpr std::array<std::array<uint8_t, 16>, 8> kCrashSeeds = {{
      {174, 25, 68, 105, 115, 116, 105, 110, 99, 116, 79, 110, 107, 137, 243,
       208},
      {174, 25, 137, 24, 162, 142, 99, 188, 19, 69, 65, 112, 99, 137, 243, 208},
      {162, 25, 142, 174, 188, 24, 174, 188, 24, 157, 118, 12, 184, 65, 208,
       208},
      {162, 25, 142, 174, 188, 24, 99, 137, 99, 137, 243, 69, 65, 208, 208,
       208},
      {162, 25, 142, 174, 188, 24, 136, 45, 243, 69, 65, 174, 188, 193, 188,
       208},
      {100, 239, 23, 195, 103, 131, 181, 146, 28, 57, 39, 204, 240, 109, 250,
       130},
      {162, 174, 24, 99, 137, 243, 69, 65, 99, 137, 243, 69, 65, 208, 208, 0},
      {162, 25, 142, 174, 75, 231, 156, 118, 12, 178, 190, 81, 188, 193, 188,
       208},
  }};
  static constexpr std::array<size_t, 8> kCrashSeedSizes = {16, 16, 16, 16,
                                                            16, 16, 15, 16};
  for (size_t i = 0; i < 8; ++i) {
    JoinFuzzTry(kCrashSeeds[i].data(), kCrashSeedSizes[i], false);
  }
}

TEST(SqlJoinFuzzer, SpillNullAwareAntiAndNonEquiRightFullJoinRegression) {
  JoinTrace trace;
  trace.setup = {
      "CREATE TABLE fact (u INT64, k INT64, a INT64);",
      "CREATE TABLE dim (k INT64, tag VARCHAR(8));",
      "INSERT INTO fact VALUES (0, 1, 10);",
      "INSERT INTO fact VALUES (1, 2, -2);",
      "INSERT INTO fact VALUES (2, 1, 0);",
      "INSERT INTO fact VALUES (3, 2, -1);",
      "INSERT INTO fact VALUES (4, 9, 3);",
      "INSERT INTO fact VALUES (5, 1, 5);",
      "INSERT INTO fact VALUES (6, NULL, 7);",
      "INSERT INTO fact VALUES (7, 2, -4);",
      "INSERT INTO fact VALUES (8, 1, 1);",
      "INSERT INTO fact VALUES (9, 2, 2);",
      "INSERT INTO dim VALUES (1, 'a');",
      "INSERT INTO dim VALUES (2, 'b');",
      "INSERT INTO dim VALUES (3, 'c');",
      "INSERT INTO dim VALUES (1, 'd');",
      "INSERT INTO dim VALUES (2, 'e');",
      "INSERT INTO dim VALUES (3, 'f');",
      "INSERT INTO dim VALUES (1, 'g');",
      "INSERT INTO dim VALUES (2, 'h');",
      "INSERT INTO dim VALUES (100, 'z');",
      "INSERT INTO dim VALUES (NULL, 'n');",
  };
  trace.queries = {
      {"SELECT f.u FROM fact f WHERE f.k NOT IN"
       " (SELECT k FROM dim WHERE k IS NOT NULL) ORDER BY f.u;",
       {"|4|,"}},
      {"SELECT COUNT(*), COUNT(f.u), COUNT(d2.tag)"
       " FROM fact f RIGHT JOIN dim d ON f.k = d.k"
       " RIGHT JOIN dim d2 ON f.a >= d2.k;",
       {"|77|,|75|,|77|,"}},
      {"SELECT COUNT(*), COUNT(f.u), COUNT(d2.tag)"
       " FROM fact f LEFT JOIN dim d ON f.k = d.k"
       " FULL JOIN dim d2 ON d.k <> d2.k;",
       {"|147|,|146|,|145|,"}},
  };
  EXPECT_EQ(ReplayJoinTrace(trace), "");
}

TEST(SqlJoinFuzzer, ReplayCommittedRegressionFiles) {
  const char* dir = std::getenv("TINYLAMB_JOIN_REGRESSION_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "skipped";
  }
  const std::vector<std::string> files = ListTestFiles(dir);
  EXPECT_FALSE(files.empty()) << "no .test files in " << dir;
  for (const std::string& path : files) {
    std::ifstream file(path);
    std::string text((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
    ASSERT_TRUE(file.good()) << path;
    uint64_t seed = 0;
    JoinTrace trace;
    std::string summary;
    ASSERT_TRUE(ParseJoinTest(text, &seed, &trace, &summary))
        << "malformed regression file: " << path;
    EXPECT_EQ(ReplayJoinTrace(trace), "")
        << "regression " << path << " still reproduces:\n"
        << summary;
  }
}

}  // namespace tinylamb
