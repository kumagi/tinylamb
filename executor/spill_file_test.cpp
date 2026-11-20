/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/spill_file.hpp"

#include <filesystem>
#include <stdexcept>
#include <utility>
#include <vector>

// setenv/unsetenv below are POSIX APIs that <cstdlib> does not declare.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <stdlib.h>

#include <algorithm>
#include <csignal>
#include <string>

// getrlimit/setrlimit below are POSIX APIs.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <sys/resource.h>

#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(SpillFileTest, AuditSemanticValueTagsSurviveSpill) {
  SpillFile spill;
  const Row original({Value("2-0 0 0:0:0").WithInterval(),
                      Value("A").WithCollation(2), Value(-1).WithUnsigned()});
  ASSERT_SUCCESS(spill.Append(original));
  ASSERT_SUCCESS(spill.FinishWriting());
  ASSIGN_OR_ASSERT_FAIL(std::vector<Row>, rows, spill.ReadAllRows());
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_TRUE(rows[0][0].IsInterval());
  EXPECT_EQ(rows[0][1].Collation(), 2);
  EXPECT_TRUE(rows[0][2].IsUnsigned());
  EXPECT_EQ(rows[0], original);
}

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
  const char* previous_temp = std::getenv("TINYLAMB_TEMP");
  const std::string saved_temp = previous_temp != nullptr ? previous_temp : "";
  std::error_code ec;
  std::filesystem::remove_all(tmp, ec);
  std::filesystem::create_directories(tmp, ec);
  ASSERT_FALSE(ec) << ec.message();
  setenv("TINYLAMB_TEMP", tmp.c_str(), 1);
  EXPECT_EQ(SpillFile::TempDirectory(), tmp);
  {
    SpillFile spill;
    ASSERT_SUCCESS(spill.Append(Row({Value(42)})));
    ASSERT_SUCCESS(spill.FinishWriting());
    EXPECT_EQ(spill.Path().parent_path(), tmp);
    EXPECT_TRUE(std::filesystem::exists(spill.Path()));
  }
  if (previous_temp != nullptr) {
    setenv("TINYLAMB_TEMP", saved_temp.c_str(), 1);
  } else {
    unsetenv("TINYLAMB_TEMP");
  }
  std::filesystem::remove_all(tmp, ec);
  EXPECT_FALSE(std::filesystem::exists(tmp));
}

TEST(SpillFileTest, ReadAllRows_WhenFileDeleted_ThrowsRuntimeError) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1)})));
  ASSERT_SUCCESS(spill.FinishWriting());
  std::filesystem::remove(spill.Path());
  EXPECT_FALSE(spill.ReadAllRows().HasValue());
}

TEST(SpillFileTest, RowReaderStreamsRowsAndTracksFileBytes) {
  SpillFile spill;
  constexpr int64_t kRows = 2500;
  for (int64_t i = 0; i < kRows; ++i) {
    ASSERT_SUCCESS(
        spill.Append(Row({Value(i), Value("val_" + std::to_string(i))})));
  }
  ASSERT_SUCCESS(spill.FinishWriting());
  EXPECT_GT(spill.FileBytes(), 0ULL);

  ASSIGN_OR_ASSERT_FAIL(SpillFile::RowReader, reader, spill.OpenRowReader());
  EXPECT_TRUE(reader.IsOpen());
  EXPECT_EQ(reader.Remaining(), static_cast<uint64_t>(kRows));
  int64_t expected = 0;
  Row row;
  bool has_row = false;
  while (true) {
    ASSERT_SUCCESS(reader.Next(&row, &has_row));
    if (!has_row) {
      break;
    }
    EXPECT_EQ(row[0].value.int_value, expected);
    EXPECT_EQ(row[1].value.varchar_value, "val_" + std::to_string(expected));
    ++expected;
  }
  EXPECT_EQ(expected, kRows);
  EXPECT_FALSE(reader.IsOpen());
  EXPECT_EQ(reader.Remaining(), 0ULL);
}

// Issue 5033: capacity failures (ENOSPC/EDQUOT on a full spill filesystem,
// or EFBIG under RLIMIT_FSIZE) used to collapse into the bare
// "spill write failed: <path>" text with no cause.  The status now embeds
// errno and maps space exhaustion to kNoSpace with an actionable hint.
TEST(SpillFileTest, WriteCapacityFailureReportsErrnoOrNoSpace) {
  struct RlimitGuard {
    rlimit old{};
    RlimitGuard() = default;
    RlimitGuard(const RlimitGuard&) = delete;
    RlimitGuard& operator=(const RlimitGuard&) = delete;
    RlimitGuard(RlimitGuard&&) = delete;
    RlimitGuard& operator=(RlimitGuard&&) = delete;
    ~RlimitGuard() { (void)setrlimit(RLIMIT_FSIZE, &old); }
  } guard;
  ASSERT_EQ(getrlimit(RLIMIT_FSIZE, &guard.old), 0);
  rlimit lim = guard.old;
  lim.rlim_cur = std::min<rlim_t>(1 << 20, lim.rlim_max);
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &lim), 0);
  // Without this, the first write past the cap raises SIGXFSZ and the
  // default disposition kills the process before errno can be observed;
  // ignoring it makes write(2) fail with EFBIG, exactly like a full disk.
  void (*previous_xfsz)(int) = std::signal(SIGXFSZ, SIG_IGN);
  struct SignalGuard {
    void (*previous)(int);
    explicit SignalGuard(void (*handler)(int)) : previous(handler) {}
    SignalGuard(const SignalGuard&) = delete;
    SignalGuard& operator=(const SignalGuard&) = delete;
    SignalGuard(SignalGuard&&) = delete;
    SignalGuard& operator=(SignalGuard&&) = delete;
    ~SignalGuard() { (void)std::signal(SIGXFSZ, previous); }
  } signal_guard{previous_xfsz};

  SpillFile spill;
  const std::string blob(4096, 'x');
  Status failure = Status::kSuccess;
  bool failed = false;
  for (int i = 0; i < 4096 && !failed; ++i) {
    failure = spill.Append(Row({Value(i), Value(std::string(blob))}));
    failed = failure != Status::kSuccess;
  }
  ASSERT_TRUE(failed) << "RLIMIT_FSIZE cap never tripped";
  const std::string text = ToString(failure);
  EXPECT_TRUE(failure.GetCode() == StatusCode::kNoSpace ||
              failure.GetCode() == StatusCode::kIOError)
      << text;
  // The whole point: the cause is visible in the text, whatever it was.
  EXPECT_NE(text.find("errno="), std::string::npos) << text;
  EXPECT_NE(text.find("tinylamb-spill-"), std::string::npos) << text;
}

}  // namespace tinylamb
