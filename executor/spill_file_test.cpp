/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/spill_file.hpp"

#include <filesystem>
#include <stdexcept>
#include <utility>
#include <vector>

// setenv/unsetenv below are POSIX APIs that <cstdlib> does not declare.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <stdlib.h>

#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(SpillFileTest, AppendAndReadAllRows) {
  SpillFile spill;
  spill.Append(Row({Value(1), Value("a")}));
  spill.Append(Row({Value(2), Value("b")}));
  spill.FinishWriting();
  ASSERT_EQ(spill.Count(), 2U);
  std::vector<Row> rows = spill.ReadAllRows();
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value("a")}));
  EXPECT_EQ(rows[1], Row({Value(2), Value("b")}));
}

TEST(SpillFileTest, EmptySpillHasZeroRows) {
  SpillFile spill;
  spill.FinishWriting();
  EXPECT_TRUE(spill.Empty());
  EXPECT_EQ(spill.Count(), 0U);
  EXPECT_TRUE(spill.ReadAllRows().empty());
}

TEST(SpillFileTest, PositionedRows) {
  SpillFile spill;
  spill.Append(Row({Value(7)}), RowPosition(1, 42));
  spill.FinishWriting();
  auto positioned = spill.ReadAllPositioned();
  ASSERT_EQ(positioned.size(), 1U);
  EXPECT_EQ(positioned[0].first, Row({Value(7)}));
  EXPECT_EQ(positioned[0].second, RowPosition(1, 42));
}

TEST(SpillFileTest, ForEachRowStreams) {
  SpillFile spill;
  for (int i = 0; i < 100; ++i) {
    spill.Append(Row({Value(i)}));
  }
  spill.FinishWriting();
  int count = 0;
  int sum = 0;
  spill.ForEachRow([&](const Row& row) {
    ++count;
    sum += row[0].value.int_value;
  });
  EXPECT_EQ(count, 100);
  EXPECT_EQ(sum, 4950);
}

TEST(SpillFileTest, ReadAllRowsAutoFinishes) {
  SpillFile spill;
  spill.Append(Row({Value(7), Value("x")}));
  spill.Append(Row({Value(8), Value("y")}));
  std::vector<Row> rows = spill.ReadAllRows();
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(7), Value("x")}));
  EXPECT_EQ(rows[1], Row({Value(8), Value("y")}));
  EXPECT_EQ(spill.Count(), 2U);
  std::vector<Row> again = spill.ReadAllRows();
  EXPECT_EQ(again.size(), 2U);
}

TEST(SpillFileTest, ReadAllPositionedAutoFinishes) {
  SpillFile spill;
  spill.Append(Row({Value(9)}), RowPosition(3, 7));
  spill.Append(Row({Value(10)}), RowPosition(4, 8));
  auto positioned = spill.ReadAllPositioned();
  ASSERT_EQ(positioned.size(), 2U);
  EXPECT_EQ(positioned[0].first, Row({Value(9)}));
  EXPECT_EQ(positioned[0].second, RowPosition(3, 7));
  EXPECT_EQ(positioned[1].second, RowPosition(4, 8));
}

TEST(SpillFileTest, AppendAfterFinishWritingThrows) {
  SpillFile spill;
  spill.Append(Row({Value(1)}));
  spill.FinishWriting();
  EXPECT_EQ(spill.Count(), 1U);
  EXPECT_THROW(spill.Append(Row({Value(2)})), std::runtime_error);
  EXPECT_THROW(spill.Append(Row({Value(2)}), RowPosition(0, 0)),
               std::runtime_error);
  EXPECT_EQ(spill.Count(), 1U);
}

TEST(SpillFileTest, ModeMismatchPlainThenPositionedThrows) {
  SpillFile spill;
  spill.Append(Row({Value(1)}));
  EXPECT_THROW(spill.Append(Row({Value(2)}), RowPosition(0, 0)),
               std::runtime_error);
}

TEST(SpillFileTest, ModeMismatchPositionedThenPlainThrows) {
  SpillFile spill;
  spill.Append(Row({Value(1)}), RowPosition(0, 0));
  EXPECT_THROW(spill.Append(Row({Value(2)})), std::runtime_error);
}

TEST(SpillFileTest, ReadAllRowsThrowsOnPositionedSpill) {
  SpillFile spill;
  spill.Append(Row({Value(1)}), RowPosition(1, 2));
  spill.FinishWriting();
  EXPECT_THROW(spill.ReadAllRows(), std::runtime_error);
}

TEST(SpillFileTest, ReadAllPositionedThrowsOnRowOnlySpill) {
  SpillFile spill;
  spill.Append(Row({Value(1)}));
  spill.FinishWriting();
  EXPECT_THROW(spill.ReadAllPositioned(), std::runtime_error);
}

TEST(SpillFileTest, ForEachRowThrowsOnPositionedSpill) {
  SpillFile spill;
  spill.Append(Row({Value(1)}), RowPosition(0, 0));
  spill.FinishWriting();
  EXPECT_THROW(spill.ForEachRow([](const Row&) {}), std::runtime_error);
}

TEST(SpillFileTest, ForEachRowOnEmptySpillIsNoop) {
  SpillFile spill;
  spill.FinishWriting();
  int calls = 0;
  spill.ForEachRow([&](const Row&) { ++calls; });
  EXPECT_EQ(calls, 0);
  EXPECT_TRUE(spill.ReadAllPositioned().empty());
  EXPECT_TRUE(spill.ReadAllRows().empty());
}

TEST(SpillFileTest, FinishWritingIsIdempotent) {
  SpillFile spill;
  spill.Append(Row({Value(3)}));
  spill.FinishWriting();
  spill.FinishWriting();
  EXPECT_TRUE(spill.Count() == 1U && !spill.Empty());
  auto rows = spill.ReadAllRows();
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(3)}));
}

namespace {

// Moves a two-row spill out of its builder so the moved-from object is
// destroyed while the destination is still alive elsewhere.
std::pair<SpillFile, std::filesystem::path> MoveTwoRowSpillOutOfScope() {
  SpillFile src;
  src.Append(Row({Value(1), Value("a")}));
  src.Append(Row({Value(2), Value("b")}));
  src.FinishWriting();
  const std::filesystem::path path = src.Path();
  SpillFile dst(std::move(src));
  return {std::move(dst), path};  // src's destructor runs on return
}

}  // namespace

TEST(SpillFileTest, MoveConstructorTransfersRows) {
  auto [dst, path] = MoveTwoRowSpillOutOfScope();
  // The moved-from destructor already ran; it must not have deleted the file.
  EXPECT_TRUE(std::filesystem::exists(path));
  EXPECT_EQ(dst.Count(), 2U);
  EXPECT_EQ(dst.Path(), path);
  auto rows = dst.ReadAllRows();
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value("a")}));
  EXPECT_EQ(rows[1], Row({Value(2), Value("b")}));
  {
    SpillFile gone(std::move(dst));
    EXPECT_EQ(gone.Count(), 2U);
    EXPECT_TRUE(std::filesystem::exists(path));
  }
  EXPECT_FALSE(std::filesystem::exists(path));
}

TEST(SpillFileTest, MoveAssignmentDeletesOldTargetFile) {
  SpillFile a;
  a.Append(Row({Value(10)}));
  a.FinishWriting();
  SpillFile b;
  b.Append(Row({Value(20)}));
  b.FinishWriting();
  const std::filesystem::path old_b = b.Path();
  EXPECT_TRUE(std::filesystem::exists(old_b));
  b = std::move(a);
  EXPECT_FALSE(std::filesystem::exists(old_b));
  // The move must empty the source file handle; asserting that contract is
  // the purpose of this test.
  EXPECT_TRUE(a.Empty());  // NOLINT(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_EQ(b.Count(), 1U);
  auto rows = b.ReadAllRows();
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(10)}));
}

TEST(SpillFileTest, SelfMoveAssignmentIsSafe) {
  SpillFile spill;
  spill.Append(Row({Value(5)}));
  spill.FinishWriting();
  const std::filesystem::path path = spill.Path();
  // Self-move must be safe; go through a reference so the compiler cannot
  // prove the self-move and warn about it.
  SpillFile& spill_alias = spill;
  spill = std::move(spill_alias);
  EXPECT_EQ(spill.Count(), 1U);
  EXPECT_TRUE(std::filesystem::exists(path));
  auto rows = spill.ReadAllRows();
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(5)}));
}

TEST(SpillFileTest, DestructorRemovesSpillFile) {
  std::filesystem::path path;
  {
    SpillFile spill;
    spill.Append(Row({Value(1), Value("z")}));
    spill.FinishWriting();
    path = spill.Path();
    EXPECT_TRUE(std::filesystem::exists(path));
  }
  EXPECT_FALSE(std::filesystem::exists(path));
}

TEST(SpillFileTest, TempDirectoryHonorsEnv) {
  const std::filesystem::path tmp =
      std::filesystem::temp_directory_path() / "tinylamb_spill_test_env";
  std::filesystem::remove_all(tmp);
  std::filesystem::create_directories(tmp);
  setenv("TINYLAMB_TEMP", tmp.c_str(), 1);
  EXPECT_EQ(SpillFile::TempDirectory(), tmp);
  {
    SpillFile spill;
    spill.Append(Row({Value(42)}));
    spill.FinishWriting();
    EXPECT_EQ(spill.Path().parent_path(), tmp);
    EXPECT_TRUE(std::filesystem::exists(spill.Path()));
  }
  unsetenv("TINYLAMB_TEMP");
  std::filesystem::remove_all(tmp);
  EXPECT_FALSE(std::filesystem::exists(tmp));
}

TEST(SpillFileTest, ReadAllFailsWhenFileDeleted) {
  SpillFile spill;
  spill.Append(Row({Value(1)}));
  spill.FinishWriting();
  std::filesystem::remove(spill.Path());
  EXPECT_THROW(spill.ReadAllRows(), std::runtime_error);
}

}  // namespace tinylamb
