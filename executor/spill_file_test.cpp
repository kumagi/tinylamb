/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/spill_file.hpp"

#include <filesystem>
#include <stdexcept>
#include <utility>
#include <vector>

// setenv/unsetenv below are POSIX APIs that <cstdlib> does not declare.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <stdlib.h>

#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(SpillFileTest, Append_MultipleRows_ReadsAllRowsCorrectly) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1), Value("a")})));
  ASSERT_SUCCESS(spill.Append(Row({Value(2), Value("b")})));
  ASSERT_SUCCESS(spill.FinishWriting());
  ASSERT_EQ(spill.Count(), 2U);
  std::vector<Row> rows = spill.ReadAllRows().MoveValue();
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(1), Value("a")}));
  EXPECT_EQ(rows[1], Row({Value(2), Value("b")}));
}

TEST(SpillFileTest, FinishWriting_OnEmptySpill_HasZeroRows) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.FinishWriting());
  EXPECT_TRUE(spill.Empty());
  EXPECT_EQ(spill.Count(), 0U);
  EXPECT_TRUE(spill.ReadAllRows().Value().empty());
}

TEST(SpillFileTest, Append_PositionedRow_ReadsPositionedRowCorrectly) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(7)}), RowPosition(1, 42)));
  ASSERT_SUCCESS(spill.FinishWriting());
  auto positioned = spill.ReadAllPositioned().MoveValue();
  ASSERT_EQ(positioned.size(), 1U);
  EXPECT_EQ(positioned[0].first, Row({Value(7)}));
  EXPECT_EQ(positioned[0].second, RowPosition(1, 42));
}

TEST(SpillFileTest, ForEachRow_ManyRows_StreamsAllRowsCorrectly) {
  SpillFile spill;
  for (int i = 0; i < 100; ++i) {
    ASSERT_SUCCESS(spill.Append(Row({Value(i)})));
  }
  ASSERT_SUCCESS(spill.FinishWriting());
  int count = 0;
  int sum = 0;
  ASSERT_SUCCESS(spill.ForEachRow([&](const Row& row) {
    ++count;
    sum += static_cast<int>(row[0].value.int_value);
  }));
  EXPECT_EQ(count, 100);
  EXPECT_EQ(sum, 4950);
}

TEST(SpillFileTest, ReadAllRows_WithoutFinishWriting_AutoFinishesAndReads) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(7), Value("x")})));
  ASSERT_SUCCESS(spill.Append(Row({Value(8), Value("y")})));
  std::vector<Row> rows = spill.ReadAllRows().MoveValue();
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(7), Value("x")}));
  EXPECT_EQ(rows[1], Row({Value(8), Value("y")}));
  EXPECT_EQ(spill.Count(), 2U);
  std::vector<Row> again = spill.ReadAllRows().MoveValue();
  EXPECT_EQ(again.size(), 2U);
}

TEST(SpillFileTest,
     ReadAllPositioned_WithoutFinishWriting_AutoFinishesAndReads) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(9)}), RowPosition(3, 7)));
  ASSERT_SUCCESS(spill.Append(Row({Value(10)}), RowPosition(4, 8)));
  auto positioned = spill.ReadAllPositioned().MoveValue();
  ASSERT_EQ(positioned.size(), 2U);
  EXPECT_EQ(positioned[0].first, Row({Value(9)}));
  EXPECT_EQ(positioned[0].second, RowPosition(3, 7));
  EXPECT_EQ(positioned[1].second, RowPosition(4, 8));
}

TEST(SpillFileTest, Append_AfterFinishWriting_ThrowsRuntimeError) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1)})));
  ASSERT_SUCCESS(spill.FinishWriting());
  EXPECT_EQ(spill.Count(), 1U);
  EXPECT_NE(spill.Append(Row({Value(2)})), Status::kSuccess);
  EXPECT_NE(spill.Append(Row({Value(2)}), RowPosition(0, 0)), Status::kSuccess);
  EXPECT_EQ(spill.Count(), 1U);
}

TEST(SpillFileTest, Append_PositionedAfterPlain_ThrowsRuntimeError) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1)})));
  EXPECT_NE(spill.Append(Row({Value(2)}), RowPosition(0, 0)), Status::kSuccess);
}

TEST(SpillFileTest, Append_PlainAfterPositioned_ThrowsRuntimeError) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1)}), RowPosition(0, 0)));
  EXPECT_NE(spill.Append(Row({Value(2)})), Status::kSuccess);
}

TEST(SpillFileTest, ReadAllRows_OnPositionedSpill_ThrowsRuntimeError) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1)}), RowPosition(1, 2)));
  ASSERT_SUCCESS(spill.FinishWriting());
  EXPECT_FALSE(spill.ReadAllRows().HasValue());
}

TEST(SpillFileTest, ReadAllPositioned_OnRowOnlySpill_ThrowsRuntimeError) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1)})));
  ASSERT_SUCCESS(spill.FinishWriting());
  EXPECT_FALSE(spill.ReadAllPositioned().HasValue());
}

TEST(SpillFileTest, ForEachRow_OnPositionedSpill_ThrowsRuntimeError) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1)}), RowPosition(0, 0)));
  ASSERT_SUCCESS(spill.FinishWriting());
  EXPECT_NE(spill.ForEachRow([](const Row&) {}), Status::kSuccess);
}

TEST(SpillFileTest, ForEachRow_OnEmptySpill_IsNoOp) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.FinishWriting());
  int calls = 0;
  ASSERT_SUCCESS(spill.ForEachRow([&](const Row&) { ++calls; }));
  EXPECT_EQ(calls, 0);
  EXPECT_TRUE(spill.ReadAllPositioned().Value().empty());
  EXPECT_TRUE(spill.ReadAllRows().Value().empty());
}

TEST(SpillFileTest, FinishWriting_CalledMultipleTimes_IsIdempotent) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(3)})));
  ASSERT_SUCCESS(spill.FinishWriting());
  ASSERT_SUCCESS(spill.FinishWriting());
  EXPECT_TRUE(spill.Count() == 1U && !spill.Empty());
  auto rows = spill.ReadAllRows().MoveValue();
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(3)}));
}

namespace {

// Moves a two-row spill out of its builder so the moved-from object is
// destroyed while the destination is still alive elsewhere.
std::pair<SpillFile, std::filesystem::path> MoveTwoRowSpillOutOfScope() {
  SpillFile src;
  EXPECT_SUCCESS(src.Append(Row({Value(1), Value("a")})));
  EXPECT_SUCCESS(src.Append(Row({Value(2), Value("b")})));
  EXPECT_SUCCESS(src.FinishWriting());
  const std::filesystem::path path = src.Path();
  SpillFile dst(std::move(src));
  return {std::move(dst), path};  // src's destructor runs on return
}

}  // namespace

TEST(SpillFileTest, MoveConstructor_FromValidSpillFile_TransfersRows) {
  auto [dst, path] = MoveTwoRowSpillOutOfScope();
  // The moved-from destructor already ran; it must not have deleted the file.
  EXPECT_TRUE(std::filesystem::exists(path));
  EXPECT_EQ(dst.Count(), 2U);
  EXPECT_EQ(dst.Path(), path);
  auto rows = dst.ReadAllRows().MoveValue();
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

TEST(SpillFileTest, MoveAssignment_ToExistingSpillFile_DeletesOldTargetFile) {
  SpillFile a;
  ASSERT_SUCCESS(a.Append(Row({Value(10)})));
  ASSERT_SUCCESS(a.FinishWriting());
  SpillFile b;
  ASSERT_SUCCESS(b.Append(Row({Value(20)})));
  ASSERT_SUCCESS(b.FinishWriting());
  const std::filesystem::path old_b = b.Path();
  EXPECT_TRUE(std::filesystem::exists(old_b));
  b = std::move(a);
  EXPECT_FALSE(std::filesystem::exists(old_b));
  // The move must empty the source file handle; asserting that contract is
  // the purpose of this test.
  EXPECT_TRUE(
      // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
      a.Empty());
  EXPECT_EQ(b.Count(), 1U);
  auto rows = b.ReadAllRows().Value();
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(10)}));
}

TEST(SpillFileTest, SelfMoveAssignment_OnValidSpillFile_PreservesState) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(5)})));
  ASSERT_SUCCESS(spill.FinishWriting());
  const std::filesystem::path path = spill.Path();
  // Self-move must be safe; go through a reference so the compiler cannot
  // prove the self-move and warn about it.
  SpillFile& spill_alias = spill;
  spill = std::move(spill_alias);
  EXPECT_EQ(spill.Count(), 1U);
  EXPECT_TRUE(std::filesystem::exists(path));
  auto rows = spill.ReadAllRows().MoveValue();
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0], Row({Value(5)}));
}

TEST(SpillFileTest, Destructor_WhenSpillFileDestroyed_RemovesUnderlyingFile) {
  std::filesystem::path path;
  {
    SpillFile spill;
    ASSERT_SUCCESS(spill.Append(Row({Value(1), Value("z")})));
    ASSERT_SUCCESS(spill.FinishWriting());
    path = spill.Path();
    EXPECT_TRUE(std::filesystem::exists(path));
  }
  EXPECT_FALSE(std::filesystem::exists(path));
}

TEST(SpillFileTest, TempDirectory_WhenEnvSet_HonorsEnvPath) {
  const std::filesystem::path tmp =
      std::filesystem::temp_directory_path() / "tinylamb_spill_test_env";
  std::filesystem::remove_all(tmp);
  std::filesystem::create_directories(tmp);
  setenv("TINYLAMB_TEMP", tmp.c_str(), 1);
  EXPECT_EQ(SpillFile::TempDirectory(), tmp);
  {
    SpillFile spill;
    ASSERT_SUCCESS(spill.Append(Row({Value(42)})));
    ASSERT_SUCCESS(spill.FinishWriting());
    EXPECT_EQ(spill.Path().parent_path(), tmp);
    EXPECT_TRUE(std::filesystem::exists(spill.Path()));
  }
  unsetenv("TINYLAMB_TEMP");
  std::filesystem::remove_all(tmp);
  EXPECT_FALSE(std::filesystem::exists(tmp));
}

TEST(SpillFileTest, ReadAllRows_WhenFileDeleted_ThrowsRuntimeError) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1)})));
  ASSERT_SUCCESS(spill.FinishWriting());
  std::filesystem::remove(spill.Path());
  EXPECT_FALSE(spill.ReadAllRows().HasValue());
}

}  // namespace tinylamb
