/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/spill_file.hpp"

#include <filesystem>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include "common/platform/process.hpp"

// setenv/unsetenv below are POSIX APIs that <cstdlib> does not declare.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <stdlib.h>

#include <algorithm>
#include <csignal>
#include <string>

// getrlimit/setrlimit below are POSIX APIs.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <sys/resource.h>

#include "common/random_string.hpp"
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

TEST(SpillFileTest, AppendRejectsOversizedStringsWithoutChangingTheFile) {
  constexpr size_t limit = std::numeric_limits<bin_size_t>::max();
  constexpr size_t kMaxOversizedTestAllocation = 1U << 20;
  if (limit >= kMaxOversizedTestAllocation) {
    GTEST_SKIP()
        << "bin_size_t limit is too large for an oversized-string test";
  }
  const Value oversized(std::string(limit + 1, 'a'));
  const Row invalid({oversized});
  const Row nested_invalid({Value::Array({oversized}, "STRING")});
  const Row supported({Value(std::string(limit, 'b'))});

  SpillFile plain;
  ASSERT_SUCCESS(plain.Append(supported));
  const uint64_t plain_bytes_before_rejection = plain.FileBytes();
  EXPECT_EQ(plain.Append(invalid).GetCode(), StatusCode::kTooBigData);
  EXPECT_EQ(plain.FileBytes(), plain_bytes_before_rejection);
  ASSERT_SUCCESS(plain.Append(supported));
  ASSIGN_OR_ASSERT_FAIL(std::vector<Row>, rows, plain.ReadAllRows());
  EXPECT_EQ(rows, (std::vector<Row>{supported, supported}));

  SpillFile positioned;
  const RowPosition location(1, 2);
  ASSERT_SUCCESS(positioned.Append(supported, location));
  const uint64_t positioned_bytes_before_rejection = positioned.FileBytes();
  EXPECT_EQ(positioned.Append(nested_invalid, location).GetCode(),
            StatusCode::kTooBigData);
  EXPECT_EQ(positioned.FileBytes(), positioned_bytes_before_rejection);
  ASSERT_SUCCESS(positioned.Append(supported, location));
  const auto result = positioned.ReadAllPositioned();
  ASSERT_TRUE(result.HasValue());
  ASSERT_EQ(result.Value().size(), 2U);
  EXPECT_EQ(result.Value()[0], std::make_pair(supported, location));
  EXPECT_EQ(result.Value()[1], std::make_pair(supported, location));
}

TEST(SpillFileTest, RowReaderRejectsMissingOutputsWithoutConsumingRow) {
  SpillFile spill;
  const Row expected({Value(42)});
  ASSERT_EQ(spill.Append(expected), Status::kSuccess);
  auto opened = spill.OpenRowReader();
  ASSERT_TRUE(opened.HasValue());
  auto reader = opened.MoveValue();
  Row row;
  bool found = false;
  EXPECT_EQ(reader.Next(nullptr, &found).GetCode(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(reader.Next(&row, nullptr).GetCode(), StatusCode::kInvalidArgument);
  ASSERT_EQ(reader.Next(&row, &found), Status::kSuccess);
  EXPECT_TRUE(found);
  EXPECT_EQ(row, expected);
}

TEST(SpillFileTest, FinishWriting_OnEmptySpill_HasZeroRows) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.FinishWriting());
  EXPECT_TRUE(spill.Empty());
  EXPECT_EQ(spill.Count(), 0U);
  ASSIGN_OR_ASSERT_FAIL(std::vector<Row>, rows, spill.ReadAllRows());
  EXPECT_TRUE(rows.empty());
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

TEST(SpillFileTest, ForEachRowStopsAtCallbackFailure) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(7)})));
  ASSERT_SUCCESS(spill.Append(Row({Value(8)})));
  ASSERT_SUCCESS(spill.FinishWriting());
  int calls = 0;
  const Status status = spill.ForEachRow([&](const Row&) {
    ++calls;
    return StatusError(StatusCode::kInvalidArgument, "callback error");
  });
  EXPECT_EQ(status.GetCode(), StatusCode::kInvalidArgument);
  EXPECT_EQ(status.GetMessage(), "callback error");
  EXPECT_EQ(calls, 1);
}

TEST(SpillFileTest, ForEachPositionedStopsAtCallbackFailure) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(7)}), RowPosition(0, 0)));
  ASSERT_SUCCESS(spill.Append(Row({Value(8)}), RowPosition(0, 1)));
  ASSERT_SUCCESS(spill.FinishWriting());
  int calls = 0;
  const Status status =
      spill.ForEachPositioned([&](const Row&, const RowPosition&) {
        ++calls;
        return StatusError(StatusCode::kInvalidArgument, "callback error");
      });
  EXPECT_EQ(status.GetCode(), StatusCode::kInvalidArgument);
  EXPECT_EQ(status.GetMessage(), "callback error");
  EXPECT_EQ(calls, 1);
}

TEST(SpillFileTest, StreamStopsAtStatusLikeCallbackFailure) {
  SpillFile rows;
  ASSERT_SUCCESS(rows.Append(Row({Value(7)})));
  ASSERT_SUCCESS(rows.Append(Row({Value(8)})));
  int row_calls = 0;
  const Status row_status = rows.ForEachRow([&](const Row&) -> StatusOr<int> {
    ++row_calls;
    return StatusError(StatusCode::kInvalidArgument, "row callback");
  });
  EXPECT_EQ(row_status.GetCode(), StatusCode::kInvalidArgument);
  EXPECT_EQ(row_status.GetMessage(), "row callback");
  EXPECT_EQ(row_calls, 1);

  SpillFile positioned;
  ASSERT_SUCCESS(positioned.Append(Row({Value(7)}), RowPosition(0, 0)));
  ASSERT_SUCCESS(positioned.Append(Row({Value(8)}), RowPosition(0, 1)));
  int positioned_calls = 0;
  const Status positioned_status =
      positioned.ForEachPositioned([&](const Row&, const RowPosition&) {
        ++positioned_calls;
        return StatusCode::kInvalidArgument;
      });
  EXPECT_EQ(positioned_status.GetCode(), StatusCode::kInvalidArgument);
  EXPECT_EQ(positioned_calls, 1);
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

TEST(SpillFileTest, Append_AfterFinishWriting_ReturnsError) {
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

TEST(SpillFileTest, ReadAllRows_OnPositionedSpill_ReturnsError) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1)}), RowPosition(1, 2)));
  ASSERT_SUCCESS(spill.FinishWriting());
  EXPECT_FALSE(spill.ReadAllRows().HasValue());
}

TEST(SpillFileTest, ReadAllPositioned_OnRowOnlySpill_ReturnsError) {
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
  auto positioned = spill.ReadAllPositioned();
  ASSERT_TRUE(positioned.HasValue());
  EXPECT_TRUE(positioned.Value().empty());
  ASSIGN_OR_ASSERT_FAIL(std::vector<Row>, rows, spill.ReadAllRows());
  EXPECT_TRUE(rows.empty());
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
  ASSIGN_OR_ASSERT_FAIL(std::vector<Row>, rows, b.ReadAllRows());
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
  // A per-test random directory keeps ctest -j runs (and reruns) from
  // colliding on the same path.  The guard restores TINYLAMB_TEMP and removes
  // the directory even when an ASSERT below returns early: both are
  // process-global state that must not leak into other tests.
  const std::filesystem::path tmp =
      std::filesystem::temp_directory_path() /
      ("tinylamb_spill_test_env_" + RandomString());
  {
    const char* previous_temp = ::tinylamb::platform::GetEnv("TINYLAMB_TEMP");
    const std::string saved_temp =
        previous_temp != nullptr ? previous_temp : "";
    struct
        TempEnvGuard {  // NOLINT(cppcoreguidelines-special-member-functions):
                        // scope-owned cleanup.
      std::filesystem::path dir;
      bool had_value;
      std::string saved;
      ~TempEnvGuard() {
        if (had_value) {
          ::tinylamb::platform::SetEnv("TINYLAMB_TEMP", saved.c_str(), 1);
        } else {
          ::tinylamb::platform::UnsetEnv("TINYLAMB_TEMP");
        }
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
      }
    } env_guard{
        .dir = tmp, .had_value = previous_temp != nullptr, .saved = saved_temp};
    std::error_code ec;
    std::filesystem::remove_all(tmp, ec);
    std::filesystem::create_directories(tmp, ec);
    ASSERT_FALSE(ec) << ec.message();
    ::tinylamb::platform::SetEnv("TINYLAMB_TEMP", tmp.c_str(), 1);
    EXPECT_EQ(SpillFile::TempDirectory(), tmp);
    {
      SpillFile spill;
      ASSERT_SUCCESS(spill.Append(Row({Value(42)})));
      ASSERT_SUCCESS(spill.FinishWriting());
      EXPECT_EQ(spill.Path().parent_path(), tmp);
      EXPECT_TRUE(std::filesystem::exists(spill.Path()));
    }
  }
  EXPECT_FALSE(std::filesystem::exists(tmp));
}

TEST(SpillFileTest, ReadAllRows_WhenFileDeleted_ReturnsError) {
  SpillFile spill;
  ASSERT_SUCCESS(spill.Append(Row({Value(1)})));
  ASSERT_SUCCESS(spill.FinishWriting());
  EXPECT_TRUE(std::filesystem::remove(spill.Path()));
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

  ASSIGN_OR_ASSERT_FAIL(SpillFile::RowReader, original, spill.OpenRowReader());
  SpillFile::RowReader moved(std::move(original));
  // NOLINTNEXTLINE(bugprone-use-after-move): moved-from reader state is tested.
  EXPECT_FALSE(original.IsOpen());
  // NOLINTNEXTLINE(bugprone-use-after-move): moved-from reader state is tested.
  EXPECT_EQ(original.Remaining(), 0U);
  ASSIGN_OR_ASSERT_FAIL(SpillFile::RowReader, reader, spill.OpenRowReader());
  reader = std::move(moved);
  // NOLINTNEXTLINE(bugprone-use-after-move): moved-from reader state is tested.
  EXPECT_FALSE(moved.IsOpen());
  // NOLINTNEXTLINE(bugprone-use-after-move): moved-from reader state is tested.
  EXPECT_EQ(moved.Remaining(), 0U);
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
  // RLIMIT_FSIZE and the SIGXFSZ disposition are process-global. gtest runs
  // the cases of this binary sequentially; keep both changes inside a small
  // scope so the guards restore them before the status assertions, and no
  // other case observes the 1 MiB cap or the ignored signal.
  Status failure = Status::kSuccess;
  bool failed = false;
  {
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
    for (int i = 0; i < 4096 && !failed; ++i) {
      failure = spill.Append(Row({Value(i), Value(std::string(blob))}));
      failed = failure != Status::kSuccess;
    }
    ASSERT_TRUE(failed) << "RLIMIT_FSIZE cap never tripped";
  }  // rlimit and SIGXFSZ restored here
  const std::string text = ToString(failure);
  EXPECT_TRUE(failure.GetCode() == StatusCode::kNoSpace ||
              failure.GetCode() == StatusCode::kIOError)
      << text;
  // The whole point: the cause is visible in the text, whatever it was.
  EXPECT_NE(text.find("errno="), std::string::npos) << text;
  EXPECT_NE(text.find("tinylamb-spill-"), std::string::npos) << text;
}

}  // namespace tinylamb
