/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"

namespace tinylamb {

namespace {

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void WriteFile(const std::filesystem::path& path, std::string_view body) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out.write(body.data(), static_cast<std::streamsize>(body.size()));
}

// The segment metadata sidecar is 20 bytes: magic, version, u64 size, CRC.
size_t ReadMetaSegmentSize(const std::string& base) {
  const std::string body = ReadFile(Logger::SegmentMetadataPath(base));
  if (body.size() != 20) {
    return 0;
  }
  // Serialization is big-endian (common/serdes.cpp).
  uint32_t magic = 0;
  for (size_t i = 0; i < 4; ++i) {
    magic = (magic << 8) | static_cast<uint8_t>(body[i]);
  }
  if (magic != 0x57414c47U) {  // 'WALG'
    return 0;
  }
  uint64_t size = 0;
  for (size_t i = 0; i < 8; ++i) {
    size = (size << 8) | static_cast<uint8_t>(body[8 + i]);
  }
  return static_cast<size_t>(size);
}

}  // namespace

class LoggerSegmentTest : public ::testing::Test {
 protected:
  void SetUp() override { base_ = "logger_segment-" + RandomString() + ".log"; }
  void TearDown() override {
    bool any = false;
    if (auto highest = Logger::FindHighestSegment(base_, &any); any) {
      if (highest.HasValue()) {
        for (uint64_t index = 0; index <= highest.Value(); ++index) {
          std::error_code ec;
          std::filesystem::remove(Logger::SegmentPath(base_, index), ec);
        }
      }
    }
    std::error_code ec;
    std::filesystem::remove(Logger::SegmentMetadataPath(base_), ec);
    std::filesystem::remove(
        Logger::SegmentMetadataPath(base_).string() + ".tmp", ec);
    // The FindHighest/Lowest test plants unrelated neighbors that must not
    // outlive the test either.
    std::filesystem::remove(base_ + ".meta", ec);
    std::filesystem::remove(base_ + ".txt", ec);
  }

  std::string base_;
};

TEST_F(LoggerSegmentTest, SegmentPathZeroIsBaseAndHigherArePadded) {
  EXPECT_EQ(Logger::SegmentPath(base_, 0), std::filesystem::path(base_));
  const std::filesystem::path first = Logger::SegmentPath(base_, 1);
  EXPECT_NE(first, base_);
  EXPECT_TRUE(first.string().starts_with(base_ + "."));
  EXPECT_EQ(first.string().size(), (base_ + ".").size() + 20);
  // Padding keeps lexicographic order aligned with numeric order.
  EXPECT_LT(Logger::SegmentPath(base_, 9).string(),
            Logger::SegmentPath(base_, 10).string());
}

TEST_F(LoggerSegmentTest, FindHighestAndLowestScanTheDirectory) {
  bool any = true;
  auto highest = Logger::FindHighestSegment(base_, &any);
  ASSERT_TRUE(highest.HasValue());
  EXPECT_EQ(highest.Value(), 0U);
  EXPECT_FALSE(any);

  WriteFile(base_, "zero");
  WriteFile(Logger::SegmentPath(base_, 3), "three");
  WriteFile(Logger::SegmentPath(base_, 7), "seven");
  // Unrelated neighbors must not join the series.
  WriteFile(base_ + ".meta", "metadata");
  WriteFile(base_ + ".txt", "unrelated");

  highest = Logger::FindHighestSegment(base_, &any);
  ASSERT_TRUE(highest.HasValue());
  EXPECT_EQ(highest.Value(), 7U);
  EXPECT_TRUE(any);
  auto lowest = Logger::FindLowestSegment(base_, &any);
  ASSERT_TRUE(lowest.HasValue());
  EXPECT_EQ(lowest.Value(), 0U);
}

TEST_F(LoggerSegmentTest, SegmentMetadataWrittenAndValidated) {
  EXPECT_EQ(ReadMetaSegmentSize(base_), 0U);  // Absent before first Create.
  auto logger = Logger::Create(base_, 4096, 1, 4096).MoveValue();
  EXPECT_EQ(ReadMetaSegmentSize(base_), 4096U);
  logger->Finish();

  // A corrupt sidecar must fail closed instead of silently re-reading a
  // wrong geometry.
  std::string body = ReadFile(Logger::SegmentMetadataPath(base_));
  ASSERT_EQ(body.size(), 20U);
  body[9] = static_cast<char>(body[9] ^ 0x40);
  WriteFile(Logger::SegmentMetadataPath(base_), body);
  auto corrupt = Logger::Create(base_, 4096, 1, 4096);
  EXPECT_FALSE(corrupt.HasValue());
}

TEST_F(LoggerSegmentTest, CreateWritesMetadataAndAppendsAcrossSegments) {
  constexpr size_t kSegmentSize = 256;
  auto logger = Logger::Create(base_, 4096, 1, kSegmentSize).MoveValue();
  EXPECT_EQ(ReadMetaSegmentSize(base_), kSegmentSize);

  // Enough records to roll past the first segment.
  lsn_t last = 0;
  const std::string payload(kSegmentSize / 2, 'x');
  for (int i = 0; i < 6; ++i) {
    auto appended = logger->AddLog(payload);
    ASSERT_TRUE(appended.HasValue());
    last = appended.Value();
  }
  EXPECT_TRUE(logger->WaitForDurable(last) == Status::kSuccess);
  EXPECT_GT(logger->CommittedLSN(), kSegmentSize);

  bool any = false;
  auto highest = Logger::FindHighestSegment(base_, &any);
  ASSERT_TRUE(highest.HasValue());
  EXPECT_GT(highest.Value(), 0U);
  EXPECT_TRUE(std::filesystem::exists(Logger::SegmentPath(base_, 1)));
  // Every segment except the newest holds exactly one segment of bytes.
  EXPECT_EQ(std::filesystem::file_size(Logger::SegmentPath(base_, 0)),
            kSegmentSize);
  EXPECT_EQ(std::filesystem::file_size(Logger::SegmentPath(base_, 1)),
            kSegmentSize);
}

TEST_F(LoggerSegmentTest, RecycleUpToRemovesWholeSegmentsBelowFloor) {
  constexpr size_t kSegmentSize = 256;
  auto logger = Logger::Create(base_, 4096, 1, kSegmentSize).MoveValue();
  lsn_t last = 0;
  const std::string payload(kSegmentSize / 2, 'y');
  for (int i = 0; i < 6; ++i) {
    auto appended = logger->AddLog(payload);
    ASSERT_TRUE(appended.HasValue());
    last = appended.Value();
  }
  EXPECT_TRUE(logger->WaitForDurable(last) == Status::kSuccess);

  // Whole-segment rule: segment (i) is removable only when
  // (i+1) * segment_size <= floor. Floor inside segment 1 removes only
  // segment 0.
  logger->SetRecycleBoundaryValidator([](lsn_t) { return true; });
  const lsn_t recycled =
      logger->RecycleUpTo(static_cast<lsn_t>(kSegmentSize) + 10);
  EXPECT_EQ(recycled, static_cast<lsn_t>(kSegmentSize));
  EXPECT_FALSE(std::filesystem::exists(Logger::SegmentPath(base_, 0)));
  EXPECT_TRUE(std::filesystem::exists(Logger::SegmentPath(base_, 1)));
  EXPECT_EQ(logger->LowestRetainedLSN(), static_cast<lsn_t>(kSegmentSize));

  // The boundary validator can pin a segment whose first byte is mid-record.
  logger->SetRecycleBoundaryValidator(
      [](lsn_t lsn) { return lsn != 2 * static_cast<lsn_t>(kSegmentSize); });
  EXPECT_EQ(logger->RecycleUpTo((static_cast<lsn_t>(kSegmentSize) * 2) +
                                kSegmentSize),
            0U);
  EXPECT_TRUE(std::filesystem::exists(Logger::SegmentPath(base_, 1)));

  // With the pin lifted, segment 1 goes and the (never-candidate) current
  // segment stays.
  logger->SetRecycleBoundaryValidator([](lsn_t) { return true; });
  const lsn_t drained = logger->RecycleUpTo(
      (static_cast<lsn_t>(kSegmentSize) * 2) + kSegmentSize);
  EXPECT_EQ(drained, static_cast<lsn_t>(kSegmentSize) * 2);
  EXPECT_TRUE(std::filesystem::exists(Logger::SegmentPath(base_, 2)));
  bool any = false;
  auto lowest = Logger::FindLowestSegment(base_, &any);
  ASSERT_TRUE(lowest.HasValue());
  EXPECT_EQ(lowest.Value(), 2U);
}

TEST_F(LoggerSegmentTest, MigrateLegacyLogFoldsOversizedTail) {
  constexpr size_t kSegmentSize = 256;
  // A pre-segmentation single-file WAL larger than one segment.
  std::string legacy((kSegmentSize * 2) + 100, 'l');
  WriteFile(base_, legacy);

  auto logger = Logger::Create(base_, 4096, 1, kSegmentSize).MoveValue();
  // The base file is trimmed to exactly one segment and the overflow moved
  // into suffixed segments.
  EXPECT_EQ(std::filesystem::file_size(base_), kSegmentSize);
  EXPECT_TRUE(std::filesystem::exists(Logger::SegmentPath(base_, 1)));
  EXPECT_TRUE(std::filesystem::exists(Logger::SegmentPath(base_, 2)));
  EXPECT_EQ(std::filesystem::file_size(Logger::SegmentPath(base_, 1)),
            kSegmentSize);
  EXPECT_EQ(std::filesystem::file_size(Logger::SegmentPath(base_, 2)), 100U);
  EXPECT_EQ(ReadMetaSegmentSize(base_), kSegmentSize);

  // The migrated bytes survive verbatim across the segment series.
  std::string folded = ReadFile(base_) +
                       ReadFile(Logger::SegmentPath(base_, 1)) +
                       ReadFile(Logger::SegmentPath(base_, 2));
  EXPECT_EQ(folded, legacy);
  lsn_t last = 0;
  auto appended = logger->AddLog("after migration");
  ASSERT_TRUE(appended.HasValue());
  last = appended.Value();
  EXPECT_TRUE(logger->WaitForDurable(last) == Status::kSuccess);
}

TEST_F(LoggerSegmentTest, ResumesLegacyMigrationAfterPartialNumberedSegment) {
  constexpr size_t kSegmentSize = 256;
  const std::string legacy((kSegmentSize * 2) + 100, 'm');
  {
    auto seed = Logger::Create(base_, 4096, 1, kSegmentSize).MoveValue();
    ASSERT_SUCCESS(seed->Finish());
  }
  WriteFile(base_, legacy);
  WriteFile(Logger::SegmentPath(base_, 1), "partial");

  auto logger = Logger::Create(base_, 4096, 1, kSegmentSize).MoveValue();
  EXPECT_EQ(std::filesystem::file_size(base_), kSegmentSize);
  EXPECT_EQ(std::filesystem::file_size(Logger::SegmentPath(base_, 1)),
            kSegmentSize);
  EXPECT_EQ(std::filesystem::file_size(Logger::SegmentPath(base_, 2)), 100U);
  const std::string folded = ReadFile(base_) +
                             ReadFile(Logger::SegmentPath(base_, 1)) +
                             ReadFile(Logger::SegmentPath(base_, 2));
  EXPECT_EQ(folded, legacy);
  ASSERT_SUCCESS(logger->Finish());
}

TEST_F(LoggerSegmentTest, TruncateToDropsHigherSegmentsAndResizesTail) {
  constexpr size_t kSegmentSize = 256;
  auto logger = Logger::Create(base_, 4096, 1, kSegmentSize).MoveValue();
  lsn_t last = 0;
  const std::string payload(kSegmentSize / 2, 'z');
  for (int i = 0; i < 6; ++i) {
    auto appended = logger->AddLog(payload);
    ASSERT_TRUE(appended.HasValue());
    last = appended.Value();
  }
  EXPECT_TRUE(logger->WaitForDurable(last) == Status::kSuccess);
  bool any = false;
  ASSERT_GT(Logger::FindHighestSegment(base_, &any).Value(), 1U);

  // Truncate back into segment 0: every higher segment must disappear and
  // the append fd must follow the truncated tail.
  const lsn_t valid_end = 100;
  EXPECT_TRUE(logger->TruncateTo(valid_end) == Status::kSuccess);
  EXPECT_FALSE(std::filesystem::exists(Logger::SegmentPath(base_, 1)));
  EXPECT_EQ(std::filesystem::file_size(base_),
            static_cast<uintmax_t>(valid_end));
  EXPECT_EQ(logger->DurableLSN(), valid_end);

  auto appended = logger->AddLog("post-truncate");
  ASSERT_TRUE(appended.HasValue());
  EXPECT_EQ(appended.Value(), valid_end);
  EXPECT_TRUE(logger->WaitForDurable(logger->BufferedLSN()) ==
              Status::kSuccess);
  EXPECT_EQ(std::filesystem::file_size(base_),
            static_cast<uintmax_t>(appended.Value()) + 13);
}

TEST_F(LoggerSegmentTest, RecordsNeverStraddleAndRecycleKeepsMoving) {
  constexpr size_t kSegmentSize = 64;
  auto logger = Logger::Create(base_, 13, 1, kSegmentSize).MoveValue();
  std::vector<std::pair<lsn_t, std::string>> records;
  for (int i = 0; i < 200; ++i) {
    const std::string payload(static_cast<size_t>(3 + ((i * 7) % 23)),
                              static_cast<char>('a' + (i % 26)));
    const lsn_t start = logger->AddLog(payload).Value();
    records.emplace_back(start, payload);
  }
  const lsn_t end = records.back().first + records.back().second.size();
  ASSERT_SUCCESS(logger->WaitForDurable(end));
  ASSERT_SUCCESS(logger->Finish());
  logger.reset();

  auto reopened = Logger::Create(base_, 13, 1, kSegmentSize).MoveValue();
  bool any = false;
  const uint64_t highest = Logger::FindHighestSegment(base_, &any).Value();
  ASSERT_TRUE(any);
  ASSERT_GT(highest, 2U);

  std::vector<lsn_t> starts;
  starts.reserve(records.size());
  for (const auto& [start, payload] : records) {
    starts.push_back(start);
    const uint64_t segment = Logger::SegmentIndex(start, kSegmentSize);
    std::ifstream input(Logger::SegmentPath(base_, segment), std::ios::binary);
    ASSERT_TRUE(input.good());
    input.seekg(static_cast<std::streamoff>(start % kSegmentSize));
    std::string actual(payload.size(), '\0');
    input.read(actual.data(), static_cast<std::streamsize>(actual.size()));
    ASSERT_EQ(input.gcount(), static_cast<std::streamsize>(actual.size()));
    ASSERT_EQ(actual, payload);
  }

  reopened->SetRecycleBoundaryValidator([&](lsn_t boundary) {
    return std::find(starts.begin(), starts.end(), boundary) != starts.end();
  });
  for (uint64_t index = 0; index < highest; ++index) {
    EXPECT_EQ(std::filesystem::file_size(Logger::SegmentPath(base_, index)),
              kSegmentSize);
  }
  for (uint64_t index = 0; index < highest; ++index) {
    const lsn_t boundary = static_cast<lsn_t>(index + 1) * kSegmentSize;
    EXPECT_EQ(reopened->RecycleUpTo(boundary), boundary);
    EXPECT_FALSE(std::filesystem::exists(Logger::SegmentPath(base_, index)));
  }
  EXPECT_EQ(reopened->LowestRetainedLSN(),
            static_cast<lsn_t>(highest) * kSegmentSize);
}

TEST_F(LoggerSegmentTest, RecoverySkipsPreRollPaddingAcrossSegments) {
  constexpr size_t kSegmentSize = 64;
  auto logger = Logger::Create(base_, 4096, 1, kSegmentSize).MoveValue();
  std::vector<lsn_t> starts;
  for (int i = 0; i < 32; ++i) {
    const auto start =
        logger->AddLog(LogRecord::BeginCheckpointLogRecord().Serialize());
    ASSERT_TRUE(start.HasValue());
    starts.push_back(start.Value());
  }
  const lsn_t expected_end = logger->BufferedLSN();
  ASSERT_SUCCESS(logger->WaitForDurable(expected_end));
  ASSERT_SUCCESS(logger->Finish());
  logger.reset();

  RecoveryManager recovery(base_, nullptr, kSegmentSize);
  EXPECT_EQ(recovery.ValidLogEnd(0), expected_end);
  size_t decoded = 0;
  for (const lsn_t start : starts) {
    LogRecord record;
    ASSERT_TRUE(recovery.ReadLog(start, &record));
    EXPECT_EQ(record.type, LogType::kBeginCheckpoint);
    ++decoded;
  }
  EXPECT_EQ(decoded, starts.size());

  bool any = false;
  const auto highest = Logger::FindHighestSegment(base_, &any);
  ASSERT_TRUE(highest.HasValue());
  ASSERT_TRUE(any);
  ASSERT_GT(highest.Value(), 2U);
  for (uint64_t index = 0; index < highest.Value(); ++index) {
    EXPECT_EQ(std::filesystem::file_size(Logger::SegmentPath(base_, index)),
              kSegmentSize);
  }
}

TEST_F(LoggerSegmentTest, RecoveryFailsClosedWhenPreRollSuccessorMissing) {
  constexpr size_t kSegmentSize = 64;
  auto logger = Logger::Create(base_, 4096, 1, kSegmentSize).MoveValue();
  for (int i = 0; i < 24; ++i) {
    ASSERT_TRUE(
        logger->AddLog(LogRecord::BeginCheckpointLogRecord().Serialize())
            .HasValue());
  }
  ASSERT_SUCCESS(logger->WaitForDurable(logger->BufferedLSN()));
  ASSERT_SUCCESS(logger->Finish());
  logger.reset();

  bool any = false;
  const auto highest = Logger::FindHighestSegment(base_, &any);
  ASSERT_TRUE(highest.HasValue());
  ASSERT_TRUE(any);
  ASSERT_GT(highest.Value(), 1U);
  ASSERT_TRUE(std::filesystem::remove(Logger::SegmentPath(base_, 1)));

  RecoveryManager recovery(base_, nullptr, kSegmentSize);
  EXPECT_EQ(recovery.ValidLogEnd(0), 0U);
  EXPECT_EQ(recovery.RecoverFrom(0, nullptr), Status::kIOError);
}

TEST_F(LoggerSegmentTest, RejectsRecordLargerThanSegment) {
  constexpr size_t kSegmentSize = 64;
  auto logger = Logger::Create(base_, 13, 1, kSegmentSize).MoveValue();
  EXPECT_FALSE(logger->AddLog(std::string(kSegmentSize + 1, 'x')).HasValue());
  EXPECT_TRUE(logger->AddLog(std::string(kSegmentSize, 'x')).HasValue());
  EXPECT_SUCCESS(logger->Finish());
}

TEST(DatabaseDeleteAllSegmentCleanupTest, DeleteAllRemovesSegmentSeries) {
  const std::string name = "delete_all_segments-" + RandomString();
  auto db = Database::Create(name).MoveValue();
  TransactionContext ctx = db->BeginContext();
  ASSERT_SUCCESS(
      db->CreateTable(ctx, Schema("t", {Column("k", ValueType::kInt64)}))
          .GetStatus());
  ASSERT_SUCCESS(ctx.PreCommit());
  // Fabricate segment artifacts the way a rolled WAL would leave them.
  const std::string segment = name + ".log.00000000000000000001";
  WriteFile(segment, "segment");
  WriteFile(name + ".log.meta", "meta");
  ASSERT_TRUE(std::filesystem::exists(segment));
  db->DeleteAll();
  EXPECT_FALSE(std::filesystem::exists(segment));
  EXPECT_FALSE(std::filesystem::exists(name + ".log.meta"));
  Database::RemoveCreatedDatabaseFiles();
}

}  // namespace tinylamb
