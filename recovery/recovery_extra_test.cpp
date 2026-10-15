/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */

// End-to-end guarantees for the segmented WAL and its recycling:
//   - records roll across segment files and read back through recovery,
//   - a legacy single-file WAL migrates into segments on first open,
//   - a durable checkpoint recycles segments that no recovery can need,
//   - a torn data-file page rebuilds from its retained full-page WAL image
//     after recycling (the FPW contract in docs/wal_format.md),
//   - a torn WAL tail across a segment boundary truncates under --force.

#include "recovery/recovery_manager.hpp"

// NOLINTNEXTLINE(modernize-deprecated-headers) POSIX setenv/unsetenv
#include <stdlib.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/serdes.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "query/sql_engine.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// Env knobs that shrink WAL segments / the page pool; must be set before
// Database::Create reads them.
constexpr const char* kSegmentEnv = "TINYLAMB_LOG_SEGMENT_BYTES";
constexpr const char* kPoolEnv = "TINYLAMB_PAGE_POOL_BYTES";
constexpr size_t kTinySegmentSize = size_t{64} * 1024;
constexpr size_t kTinyPoolBytes = size_t{8} * size_t{kPageSize};

class ScopedEnv {
 public:
  ScopedEnv(const char* name, const std::string& value) : name_(name) {
    const char* old = std::getenv(name);
    if (old != nullptr) {
      old_ = old;
    }
    setenv(name, value.c_str(), 1);
  }
  ~ScopedEnv() {
    if (old_.has_value()) {
      setenv(name_, old_->c_str(), 1);
    } else {
      unsetenv(name_);
    }
  }
  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;
  ScopedEnv(ScopedEnv&&) = delete;
  ScopedEnv& operator=(ScopedEnv&&) = delete;

 private:
  const char* name_;
  std::optional<std::string> old_;
};

// Segment indices present for `base` right now.
std::vector<uint64_t> ExistingSegments(const std::string& base) {
  std::vector<uint64_t> found;
  const std::string prefix = base + ".log.";
  std::error_code ec;
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(".", ec)) {
    if (ec) {
      break;
    }
    const std::string name = entry.path().filename().string();
    if (!name.starts_with(prefix)) {
      continue;
    }
    const std::string suffix = name.substr(prefix.size());
    if (suffix.empty() ||
        suffix.find_first_not_of("0123456789") != std::string::npos) {
      continue;
    }
    found.push_back(std::stoull(suffix));
  }
  std::sort(found.begin(), found.end());
  return found;
}

size_t SegmentFileBytes(const std::string& logfile, uint64_t index) {
  std::error_code ec;
  const auto size =
      std::filesystem::file_size(Logger::SegmentPath(logfile, index), ec);
  return ec ? 0 : static_cast<size_t>(size);
}

size_t WALFileBytes(const std::string& base) {
  // Segment 0 is the bare base file (see Logger::SegmentPath); suffixed
  // segments follow.
  size_t total = SegmentFileBytes(base + ".log", 0);
  for (const uint64_t index : ExistingSegments(base)) {
    total += SegmentFileBytes(base + ".log", index);
  }
  return total;
}

// Decodes the LSN out of a master record file. Mirrors the 20-byte layout
// written by CheckpointManager (magic, version, LSN, CRC); the test only
// needs the LSN and the magic, not the checksum.
lsn_t MasterRecordLsn(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  EXPECT_FALSE(in.fail()) << path;
  std::array<char, kCheckpointMasterRecordSize> encoded{};
  in.read(encoded.data(), static_cast<std::streamsize>(encoded.size()));
  EXPECT_EQ(in.gcount(), static_cast<std::streamsize>(encoded.size()));
  uint32_t magic = 0;
  uint32_t version = 0;
  uint64_t lsn = 0;
  DeserializeU32(encoded.data(), &magic);
  DeserializeU32(encoded.data() + sizeof(uint32_t), &version);
  DeserializeU64(encoded.data() + (sizeof(uint32_t) * 2), &lsn);
  EXPECT_EQ(magic, kCheckpointMasterMagic);
  EXPECT_EQ(version, kCheckpointMasterVersion);
  return static_cast<lsn_t>(lsn);
}

Status RunSql(Database& db, const std::string& sql) {
  SqlEngine engine(db);
  TransactionContext ctx = db.BeginContext();
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  if (!prepared.HasValue()) {
    ADD_FAILURE() << sql << "\n" << engine.LastError();
    return prepared.GetStatus();
  }
  Row row;
  bool any = prepared.Value()->Next(&row, nullptr);
  if (!any) {
    Status stream_status = prepared.Value()->GetStatus();
    if (stream_status != Status::kSuccess) {
      ADD_FAILURE() << sql << "\nstream status: " << stream_status << " "
                    << engine.LastError();
      static_cast<void>(ctx.txn_.Abort());
      return stream_status;
    }
  }
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  return ctx.txn_.PreCommit();
}

std::vector<int64_t> SelectKeys(Database& db, const std::string& table = "t") {
  SqlEngine engine(db);
  TransactionContext ctx = db.BeginContext();
  StatusOr<Executor> prepared =
      engine.Prepare(ctx, "SELECT k FROM " + table + ";");
  EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << engine.LastError();
  std::vector<int64_t> keys;
  if (!prepared.HasValue()) {
    return keys;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    keys.push_back(row[0].value.int_value);
  }
  EXPECT_SUCCESS(ctx.txn_.PreCommit());
  return keys;
}

size_t CountRows(Database& db, const std::string& table = "t") {
  return SelectKeys(db, table).size();
}

// Single-value probe for the committed-value survival tests: returns the
// first column of the first row, or an error when the row is absent.
StatusOr<int64_t> SelectScalar(Database& db, const std::string& sql) {
  SqlEngine engine(db);
  TransactionContext ctx = db.BeginContext();
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  if (!prepared.HasValue()) {
    return prepared.GetStatus();
  }
  Row row;
  if (!prepared.Value()->Next(&row, nullptr)) {
    const Status stream_status = prepared.Value()->GetStatus();
    if (stream_status != Status::kSuccess) {
      static_cast<void>(ctx.txn_.Abort());
      return stream_status;
    }
    static_cast<void>(ctx.txn_.PreCommit());
    return StatusError(StatusCode::kNotExists, "no row: " + sql);
  }
  const int64_t value = row[0].value.int_value;
  const Status commit = ctx.txn_.PreCommit();
  if (commit != Status::kSuccess) {
    return commit;
  }
  return value;
}

std::string BigPayload() {
  // ~1.5 KiB per row: a few hundred rows overflow a small buffer pool, so
  // pages actually evict and flush during the test.
  return std::string(1500, 'x');  // NOLINT(modernize-return-braced-init-list)
}

// Overwrites the last `pages_to_corrupt` live page images in `db_file` with
// garbage -- the on-disk equivalent of a torn multi-page write burst. Only
// high page ids are targeted: the newest allocations, whose images were
// flushed (and therefore WAL-snapshotted) most recently.
void CorruptLastLivePages(const std::string& db_file, size_t pages_to_corrupt) {
  std::fstream file(db_file, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_FALSE(file.fail());
  file.seekg(0, std::ios_base::end);
  const auto file_size = file.tellg();
  const size_t page_count =
      static_cast<size_t>(file_size) / static_cast<size_t>(kPageSize);
  size_t corrupted = 0;
  for (size_t index = page_count; index > 1 && corrupted < pages_to_corrupt;
       --index) {
    const size_t page_id = index - 1;
    std::vector<char> image(kPageSize);
    file.seekg(static_cast<std::streamoff>(page_id) *
                   static_cast<std::streamoff>(kPageSize),
               std::ios_base::beg);
    file.read(image.data(), static_cast<std::streamsize>(image.size()));
    if (file.gcount() != static_cast<std::streamsize>(kPageSize)) {
      continue;
    }
    const bool all_zero =
        std::ranges::all_of(image, [](char byte) { return byte == 0; });
    if (all_zero) {
      continue;  // Sparse tail: not a live page.
    }
    for (size_t offset = kPageHeaderSize; offset < kPageSize; ++offset) {
      image[offset] = static_cast<char>(0xA5 ^ offset);
    }
    file.seekp(static_cast<std::streamoff>(page_id) *
                   static_cast<std::streamoff>(kPageSize),
               std::ios_base::beg);
    file.write(image.data(), static_cast<std::streamsize>(image.size()));
    ++corrupted;
  }
  ASSERT_GT(corrupted, size_t{0}) << "no live page found to corrupt";
}

}  // namespace

class RecoveryExtraTest : public ::testing::Test {
 protected:
  void SetUp() override { prefix_ = "recovery_extra_test-" + RandomString(); }
  void TearDown() override {
    if (db_ != nullptr) {
      db_->DeleteAll();
      db_.reset();
    }
    RemoveLogAndSegments(prefix_ + ".log");
  }

  // Drops buffered pages and restarts the whole stack: the next Create runs
  // restart recovery exactly like a crashed process would.
  Database& Reopen() {
    if (db_ != nullptr) {
      db_->EmulateCrash();
      db_.reset();
    }
    StatusOr<std::unique_ptr<Database>> created = Database::Create(prefix_);
    EXPECT_EQ(created.GetStatus(), Status::kSuccess)
        << created.GetStatus() << " msg=[" << created.GetStatus().GetMessage()
        << "]";
    db_ = created.MoveValue();
    EXPECT_NE(db_, nullptr);
    return *db_;
  }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};

TEST_F(RecoveryExtraTest, RejectsSegmentTooSmallForFullPageImage) {
  ScopedEnv segment_env(kSegmentEnv, std::to_string(kPageSize));
  StatusOr<std::unique_ptr<Database>> created = Database::Create(prefix_);
  EXPECT_FALSE(created.HasValue());
  EXPECT_EQ(created.GetStatus(), Status::kInvalidArgument);
  std::error_code remove_error;
  std::filesystem::remove(prefix_ + ".db", remove_error);
}

TEST_F(RecoveryExtraTest, WalRollsAcrossSegmentsAndReadsBack) {
  ScopedEnv segment_env(kSegmentEnv, std::to_string(kTinySegmentSize));
  const std::string base = prefix_ + ".log";
  std::unique_ptr<Logger> logger =
      Logger::Create(base, kTinySegmentSize, 1, kTinySegmentSize).MoveValue();
  ASSERT_NE(logger, nullptr);
  const std::string payload(4096, 'x');
  std::vector<lsn_t> record_starts;
  for (int i = 0; i < 48; ++i) {
    const LogRecord record = LogRecord::FullPageImageLogRecord(
        static_cast<page_id_t>(i), PageType::kRowPage, payload);
    const std::string serialized = record.Serialize();
    ASSIGN_OR_CRASH(lsn_t, start, logger->AddLog(serialized));
    record_starts.push_back(start);
  }
  ASSERT_SUCCESS(logger->WaitForDurable(logger->BufferedLSN()));
  const lsn_t end = logger->BufferedLSN();
  EXPECT_GT(end, static_cast<lsn_t>(2 * kTinySegmentSize))
      << "test must span at least two segment boundaries";
  logger->Finish();
  logger.reset();

  EXPECT_GT(ExistingSegments(prefix_).size(), size_t{2})
      << "expected several segment files after the roll";

  // Every record must decode back at its absolute LSN, including ones that
  // straddle segment boundaries.
  RecoveryManager recovery(base, nullptr, kTinySegmentSize);
  for (size_t i = 0; i < record_starts.size(); ++i) {
    LogRecord decoded;
    ASSERT_TRUE(recovery.ReadLog(record_starts[i], &decoded))
        << "record " << i << " at LSN " << record_starts[i];
    EXPECT_EQ(decoded.type, LogType::kFullPageImage);
    EXPECT_EQ(decoded.pid, static_cast<page_id_t>(i));
    EXPECT_EQ(decoded.redo_data.size(), payload.size());
  }
  EXPECT_EQ(recovery.ValidLogEnd(0), end);
}

TEST_F(RecoveryExtraTest, PersistedSegmentGeometryWinsOnReopen) {
  const std::string logfile = prefix_ + ".log";
  constexpr size_t kChangedSegmentSize = kTinySegmentSize / 2;
  const std::string payload(4096, 'z');
  lsn_t end = 0;
  {
    auto logger = Logger::Create(logfile, kTinySegmentSize, 1, kTinySegmentSize)
                      .MoveValue();
    ASSERT_NE(logger, nullptr);
    for (int i = 0; i < 48; ++i) {
      const LogRecord record = LogRecord::FullPageImageLogRecord(
          static_cast<page_id_t>(i), PageType::kRowPage, payload);
      ASSERT_SUCCESS(logger->AddLog(record.Serialize()).GetStatus());
    }
    ASSERT_SUCCESS(logger->WaitForDurable(logger->BufferedLSN()));
    end = logger->BufferedLSN();
    ASSERT_SUCCESS(logger->Finish());
  }
  ASSERT_TRUE(std::filesystem::exists(Logger::SegmentMetadataPath(logfile)));
  const std::vector<uint64_t> segments_before = ExistingSegments(prefix_);
  ASSERT_FALSE(segments_before.empty());

  auto reopened =
      Logger::Create(logfile, kTinySegmentSize, 1, kChangedSegmentSize)
          .MoveValue();
  ASSERT_NE(reopened, nullptr);
  EXPECT_EQ(reopened->SegmentSize(), kTinySegmentSize);
  EXPECT_EQ(ExistingSegments(prefix_), segments_before);
  EXPECT_EQ(reopened->BufferedLSN(), end);
  EXPECT_SUCCESS(reopened->Finish());
}

TEST_F(RecoveryExtraTest, DatabaseReopenUsesPersistedSegmentGeometry) {
  {
    ScopedEnv segment_env(kSegmentEnv, std::to_string(kTinySegmentSize));
    Database& db = Reopen();
    EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64);"));
    for (int i = 0; i < 1000; ++i) {
      EXPECT_SUCCESS(
          RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) + ");"));
    }
    ASSERT_FALSE(ExistingSegments(prefix_).empty());
  }
  ScopedEnv changed_env(kSegmentEnv, std::to_string(kTinySegmentSize / 2));
  Database& reopened = Reopen();
  EXPECT_EQ(CountRows(reopened), size_t{1000});
}

TEST_F(RecoveryExtraTest, MissingGeometryDoesNotMigrateSegmentedWal) {
  const std::string logfile = prefix_ + ".log";
  {
    auto logger = Logger::Create(logfile, kTinySegmentSize, 1, kTinySegmentSize)
                      .MoveValue();
    ASSERT_NE(logger, nullptr);
    const std::string payload(4096, 'q');
    for (int i = 0; i < 48; ++i) {
      const LogRecord record = LogRecord::FullPageImageLogRecord(
          static_cast<page_id_t>(i), PageType::kRowPage, payload);
      ASSERT_SUCCESS(logger->AddLog(record.Serialize()).GetStatus());
    }
    ASSERT_SUCCESS(logger->WaitForDurable(logger->BufferedLSN()));
    ASSERT_SUCCESS(logger->Finish());
  }
  bool any_segment = false;
  const uint64_t highest =
      Logger::FindHighestSegment(logfile, &any_segment).MoveValue();
  ASSERT_TRUE(any_segment);
  ASSERT_GT(highest, uint64_t{0});
  const auto segment_path = Logger::SegmentPath(logfile, highest);
  const auto bytes_before = std::filesystem::file_size(segment_path);
  ASSERT_TRUE(std::filesystem::remove(Logger::SegmentMetadataPath(logfile)));

  const StatusOr<std::unique_ptr<Logger>> reopened =
      Logger::Create(logfile, kTinySegmentSize, 1, kTinySegmentSize / 2);
  EXPECT_EQ(reopened.GetStatus(), Status::kIOError);
  EXPECT_EQ(std::filesystem::file_size(segment_path), bytes_before);
}

TEST_F(RecoveryExtraTest, CorruptSegmentGeometryFailsClosed) {
  const std::string logfile = prefix_ + ".log";
  {
    auto logger = Logger::Create(logfile, kTinySegmentSize, 1, kTinySegmentSize)
                      .MoveValue();
    ASSERT_NE(logger, nullptr);
    ASSERT_SUCCESS(logger->Finish());
  }
  const std::filesystem::path metadata = Logger::SegmentMetadataPath(logfile);
  {
    std::ofstream corrupt(metadata, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(corrupt.good());
    corrupt << "bad";
  }
  const StatusOr<std::unique_ptr<Logger>> reopened =
      Logger::Create(logfile, kTinySegmentSize, 1, kTinySegmentSize);
  EXPECT_EQ(reopened.GetStatus(), Status::kCorrupt);
}

TEST_F(RecoveryExtraTest, LegacySingleFileLogMigratesToSegments) {
  const std::string logfile = prefix_ + ".log";
  // Fabricate a pre-segmentation WAL: a single byte file larger than the
  // target segment size. Writing through a segment size of 4 MiB produces
  // exactly that: one bare file holding all records.
  {
    auto legacy =
        Logger::Create(logfile, kTinySegmentSize, 1, size_t{4} * 1024 * 1024)
            .MoveValue();
    ASSERT_NE(legacy, nullptr);
    const std::string payload(4096, 'y');
    for (int i = 0; i < 24; ++i) {
      const LogRecord record = LogRecord::FullPageImageLogRecord(
          static_cast<page_id_t>(i), PageType::kRowPage, payload);
      EXPECT_SUCCESS(legacy->AddLog(record.Serialize()).GetStatus());
    }
    ASSERT_SUCCESS(legacy->WaitForDurable(legacy->BufferedLSN()));
    legacy->Finish();
    legacy.reset();
  }
  std::error_code ec;
  ASSERT_TRUE(std::filesystem::exists(logfile, ec));
  const auto legacy_size = std::filesystem::file_size(logfile, ec);
  ASSERT_FALSE(ec);
  ASSERT_GT(legacy_size, static_cast<uintmax_t>(kTinySegmentSize))
      << "the fabricated log must overflow one target segment";
  ASSERT_TRUE(std::filesystem::remove(Logger::SegmentMetadataPath(logfile)));

  // Opening with a smaller segment size must fold the overflow into
  // suffixed segments and trim the base file to one segment.
  auto logger = Logger::Create(logfile, kTinySegmentSize, 1, kTinySegmentSize)
                    .MoveValue();
  ASSERT_NE(logger, nullptr);
  EXPECT_EQ(std::filesystem::file_size(logfile, ec),
            static_cast<uintmax_t>(kTinySegmentSize))
      << "base file (segment 0) was not trimmed to one segment";
  EXPECT_GE(ExistingSegments(prefix_).size(), size_t{1})
      << "migration should have created suffixed overflow segments";
  EXPECT_EQ(logger->BufferedLSN(), static_cast<lsn_t>(legacy_size));

  // All 24 records survive at their absolute LSNs.
  RecoveryManager recovery(logfile, nullptr, kTinySegmentSize);
  size_t readable = 0;
  for (auto offset = lsn_t{0}; offset < recovery.LogEnd();) {
    LogRecord decoded;
    if (!recovery.ReadLog(offset, &decoded)) {
      break;
    }
    ++readable;
    offset += decoded.Size();
  }
  EXPECT_EQ(readable, size_t{24});
  logger->Finish();
}

TEST_F(RecoveryExtraTest, CheckpointRecyclesOldSegmentsAndRestarts) {
  ScopedEnv segment_env(kSegmentEnv, std::to_string(kTinySegmentSize));
  ScopedEnv pool_env(kPoolEnv, std::to_string(kTinyPoolBytes));
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64);"));
  for (int i = 0; i < 400; ++i) {
    EXPECT_SUCCESS(
        RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) + ");"));
  }

  // Durable checkpoint: everything below the new anchor becomes recyclable.
  StatusOr<lsn_t> first = db.WriteCheckpoint();
  ASSERT_SUCCESS(first.GetStatus());
  EXPECT_GT(first.Value(), lsn_t{0});
  const size_t bytes_after_first = WALFileBytes(prefix_);
  EXPECT_GT(WALFileBytes(prefix_), size_t{0});

  // A second write generation plus another checkpoint: the retained window
  // must track the working set, not the database age.
  for (int i = 400; i < 800; ++i) {
    EXPECT_SUCCESS(
        RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) + ");"));
  }
  StatusOr<lsn_t> second = db.WriteCheckpoint();
  ASSERT_SUCCESS(second.GetStatus());
  const std::vector<uint64_t> segments_after_second = ExistingSegments(prefix_);
  ASSERT_FALSE(segments_after_second.empty());
  LOG(INFO) << "segments after first checkpoint: "
            << ExistingSegments(prefix_).size() << " (" << bytes_after_first
            << " bytes), after second: " << segments_after_second.size() << " ("
            << WALFileBytes(prefix_) << " bytes)";
  // Bounded retention: after two checkpoints the WAL holds at most a few
  // segments around the working set, not the whole history (the unbounded
  // legacy behavior grew linearly with total writes).
  EXPECT_LE(WALFileBytes(prefix_), size_t{4} * kTinySegmentSize)
      << "WAL did not shrink across checkpoints";

  // Restart recovery over the recycled WAL: both write generations survive.
  Database& reopened = Reopen();
  EXPECT_EQ(CountRows(reopened), size_t{800});
}

// Regression for the sustained-load recycle stall: with page write-backs in
// flight at every data-file sync and a continuously rewritten hot page,
// checkpoints used to succeed forever while NOTHING was ever recycled -- the
// base segment survived the whole run and the WAL grew at the full write
// rate (~8.5 MB/s observed on TPC-C, ~700 GB/day). Repeated checkpoints
// under concurrent writers must keep the retained WAL at a few segments
// around the working set.
TEST_F(RecoveryExtraTest,
       CheckpointKeepsRecyclingUnderSustainedConcurrentLoad) {
  ScopedEnv segment_env(kSegmentEnv, std::to_string(kTinySegmentSize));
  ScopedEnv pool_env(kPoolEnv, std::to_string(kTinyPoolBytes));
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64 PRIMARY KEY, v INT64);"));
  for (int i = 0; i < 100; ++i) {
    EXPECT_SUCCESS(
        RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) + ", 0);"));
  }

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> updates{0};
  std::vector<std::thread> writers;
  writers.reserve(4);
  for (size_t w = 0; w < 4; ++w) {
    writers.emplace_back([&, w] {
      uint64_t seq = w * 9'77;
      while (!stop.load(std::memory_order_relaxed)) {
        ++seq;
        const int key = static_cast<int>(seq % 100);
        // Conflicting updates under the legacy wait policy abort with
        // kConflicts; that is expected contention, not a regression -- the
        // statement result is irrelevant here, only WAL behavior matters.
        const Status updated = RunSql(db, "UPDATE t SET v = v + 1 WHERE k = " +
                                              std::to_string(key) + ";");
        if (updated == Status::kSuccess) {
          updates.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (int round = 0; round < 6; ++round) {
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    const StatusOr<lsn_t> ckpt = db.WriteCheckpoint();
    ASSERT_SUCCESS(ckpt.GetStatus());
  }
  stop.store(true, std::memory_order_relaxed);
  for (std::thread& writer : writers) {
    writer.join();
  }
  EXPECT_GT(updates.load(), uint64_t{0}) << "writers made no progress";

  // The retained WAL must track the working set, not the number of
  // checkpoints or writes performed.
  const size_t retained = WALFileBytes(prefix_);
  LOG(INFO) << "retained WAL under load: " << retained << " bytes, "
            << ExistingSegments(prefix_).size() << " segments, "
            << updates.load() << " updates";
  EXPECT_LE(retained, size_t{8} * kTinySegmentSize)
      << "WAL recycling stalled under sustained load";

  // The database stays recoverable over the recycled WAL.
  Database& reopened = Reopen();
  EXPECT_EQ(CountRows(reopened), size_t{100});
}

// Regression: the pre-anchor commit prescan used to stop silently at the
// FIRST segment-tail padding region, while the redo pass still replayed from
// the dirty-page-table recLSN floor below the anchor. Every transaction
// committed between the first padding and the anchor then looked uncommitted
// to the per-page undo phase, which physically deleted its rows (and
// compensated its page allocations). A checkpointed heavy load lost millions
// of rows on the next restart: "full scan reached a non-row page", counts
// shrinking from 6,001,215 to 850,000.
//
// The shape needs three things at once, so the test builds them explicitly:
//   - a recycled WAL prefix (log_start > 0), otherwise the analysis pass
//     covers the whole history and no prescan runs;
//   - dirty pages whose recLSN reaches below the checkpoint anchor, so the
//     redo window contains pre-anchor committed records (checkpointing with
//     the flush phase disabled keeps every page dirty);
//   - segment-tail padding between the recycled start and those commits
//     (tiny segments put a padding region every kTinySegmentSize bytes).
TEST_F(RecoveryExtraTest, PrescanSurvivesSegmentPaddingWhenRecycling) {
  ScopedEnv segment_env(kSegmentEnv, std::to_string(kTinySegmentSize));
  // Phase 1 runs with the default flush budget so the first checkpoint
  // cleans every page: its DPT empties, the recycle floor reaches the
  // anchor, and the retained prefix starts ABOVE zero -- which is what arms
  // the prescan for the second checkpoint's recovery.
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64, v INT64);"));
  for (int i = 0; i < 400; ++i) {
    EXPECT_SUCCESS(
        RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) + ", 0);"));
  }
  ASSERT_SUCCESS(db.WriteCheckpoint().GetStatus());
  {
    Database& reopened = Reopen();
    EXPECT_EQ(CountRows(reopened), size_t{400});
  }

  // Phase 2 re-dirties the row pages with the flush phase disabled: their
  // recLSNs stay above the first checkpoint (so the WAL prefix stays
  // recycled) while their committed records sit below the second anchor (so
  // the redo window still contains them). The shape matters twice over: the
  // redo pass replays in ~16k-record batches and only knows commits up to
  // each batch's flush point, so a transaction whose record run CROSSES a
  // batch boundary is replayed-and-undone before its commit is collected --
  // unless the prescan already classified it. Twelve 3000-row statements put
  // multi-thousand-record transactions right across the first boundaries.
  ScopedEnv no_flush("TINYLAMB_CHECKPOINT_FLUSH_MS", "0");
  Database& dirty = Reopen();
  for (int stmt = 0; stmt < 12; ++stmt) {
    std::string sql = "INSERT INTO t VALUES ";
    for (int j = 0; j < 3000; ++j) {
      const int k = 400 + stmt * 1000 + j;
      if (j != 0) {
        sql += ", ";
      }
      sql += "(" + std::to_string(k) + ", 1)";
    }
    sql += ";";
    EXPECT_SUCCESS(RunSql(dirty, sql));
  }
  ASSERT_SUCCESS(dirty.WriteCheckpoint().GetStatus());

  Database& reopened = Reopen();
  EXPECT_EQ(CountRows(reopened), size_t{36400});
  const StatusOr<int64_t> updated =
      SelectScalar(reopened, "SELECT COUNT(*) FROM t WHERE v = 1;");
  ASSERT_SUCCESS(updated.GetStatus());
  EXPECT_EQ(updated.Value(), 36000);
}

// The flush phase writes B+tree pages mid-lifetime (leaf splits, foster
// churn, branch separators), which plain buffer-pool eviction may never do
// when the pool holds the whole working set. Concurrent inserts that grow
// the tree, interleaved with checkpoints and a tiny pool, must leave the
// index coherent in memory AND across a restart over the recycled WAL.
TEST_F(RecoveryExtraTest,
       CheckpointFlushKeepsTreeCoherentUnderConcurrentInserts) {
  ScopedEnv segment_env(kSegmentEnv, std::to_string(kTinySegmentSize));
  ScopedEnv pool_env(kPoolEnv, std::to_string(kTinyPoolBytes));
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64 PRIMARY KEY);"));

  std::atomic<bool> stop{false};
  std::atomic<int64_t> next_key{0};
  std::atomic<uint64_t> inserted{0};
  std::vector<std::thread> writers;
  writers.reserve(4);
  for (size_t w = 0; w < 4; ++w) {
    writers.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        const int64_t key = next_key.fetch_add(1, std::memory_order_relaxed);
        // Four concurrent appenders plus an 8-page pool legitimately
        // produce transactional conflicts (a concurrent tail growth, an
        // index-undo reservation). A conflict aborts only THIS statement:
        // retry it -- the test's contract is that the tree stays coherent
        // over checkpoints and recycled WAL, not that racing appenders
        // never serialize.
        Status inserted_status = Status::kUnknown;
        for (int attempt = 0; attempt < 8; ++attempt) {
          inserted_status =
              RunSql(db, "INSERT INTO t VALUES (" + std::to_string(key) + ");");
          if (inserted_status != Status::kConflicts) {
            break;
          }
          std::this_thread::yield();
        }
        if (inserted_status == Status::kSuccess) {
          inserted.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (int round = 0; round < 6; ++round) {
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    const StatusOr<lsn_t> ckpt = db.WriteCheckpoint();
    ASSERT_SUCCESS(ckpt.GetStatus());
  }
  stop.store(true, std::memory_order_relaxed);
  for (std::thread& writer : writers) {
    writer.join();
  }
  EXPECT_GT(inserted.load(), uint64_t{100}) << "inserts made no progress";

  // In-memory navigation stays coherent: a full index scan over the
  // split-heavy tree returns exactly the committed keys.
  const std::vector<int64_t> keys = SelectKeys(db);
  EXPECT_EQ(keys.size(), inserted.load());

  // And the tree rebuilds identically over the recycled WAL.
  Database& reopened = Reopen();
  const std::vector<int64_t> reopened_keys = SelectKeys(reopened);
  ASSERT_EQ(reopened_keys.size(), inserted.load());
  EXPECT_TRUE(std::ranges::is_sorted(reopened_keys));
}

TEST_F(RecoveryExtraTest, TornPageRebuildsFromFullPageImageAfterRecycle) {
  ScopedEnv segment_env(kSegmentEnv, std::to_string(kTinySegmentSize));
  ScopedEnv pool_env(kPoolEnv, std::to_string(kTinyPoolBytes));
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64, v VARCHAR);"));
  const std::string payload = BigPayload();
  for (int i = 0; i < 300; ++i) {
    EXPECT_SUCCESS(RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) +
                                  ", '" + payload + "');"));
  }
  // Anchor a checkpoint (data file fsync + recycle).
  StatusOr<lsn_t> first = db.WriteCheckpoint();
  ASSERT_SUCCESS(first.GetStatus());
  // A second write generation: these pages flush AFTER the checkpoint, so
  // their full-page images sit above the recycle floor.
  for (int i = 300; i < 500; ++i) {
    EXPECT_SUCCESS(RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) +
                                  ", '" + payload + "');"));
  }
  EXPECT_SUCCESS(RunSql(db, "DELETE FROM t WHERE k < 50;"));
  EXPECT_EQ(CountRows(db), size_t{450});

  // Simulate a crash that tore the most recent page-image writes, then
  // reopen. Recovery must rebuild every corrupted page from the WAL.
  db.EmulateCrash();
  CorruptLastLivePages(prefix_ + ".db", 3);
  Database& reopened = Reopen();
  EXPECT_EQ(CountRows(reopened), size_t{450});
  const std::vector<int64_t> keys = SelectKeys(reopened);
  EXPECT_EQ(keys.size(), size_t{450});
  EXPECT_EQ(std::ranges::count(keys, int64_t{499}), 1);
}

// Regression guard for the meta-page re-entrancy in RecoverFrom: rebuilding a
// torn page replays allocation records through PageManager::GetMetaPage(), and
// page 0 carries its own full-page image, so the meta page is normally a
// dirty-page-table member. Latching the valid dirty pages before running the
// rebuilds made the recovery thread re-lock a std::shared_mutex it already
// owned; libstdc++ reports that as
// std::system_error(resource_deadlock_would_occur), which surfaced as an
// intermittent "Resource deadlock avoided" in
// TornPageRebuildsFromFullPageImageAfterRecycle. Tearing several pages at once
// across repeated crash/reopen cycles widens the pre-fix interleaving window;
// with the rebuilds ordered ahead of the latching loop every order succeeds.
// A crash that tears several pages must not cost the database its catalog.
// Recovery mutates pages (redo, loser undo, free-list relink) and the stored
// checksum then no longer described the body, so a repaired page still read as
// TORN: PageManager::GetPage returned an empty ref and BPlusTree::Open
// bootstrapped a fresh, empty tree -- the tables silently vanished. Tearing a
// wider set of pages exercises the same path deterministically.
TEST_F(RecoveryExtraTest, TornPagesOutsideDirtyTableKeepTheCatalog) {
  ScopedEnv segment_env(kSegmentEnv, std::to_string(kTinySegmentSize));
  ScopedEnv pool_env(kPoolEnv, std::to_string(kTinyPoolBytes));
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64, v VARCHAR);"));
  const std::string payload = BigPayload();
  for (int i = 0; i < 150; ++i) {
    EXPECT_SUCCESS(RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) +
                                  ", '" + payload + "');"));
  }
  ASSERT_SUCCESS(db.WriteCheckpoint().GetStatus());
  for (int i = 150; i < 240; ++i) {
    EXPECT_SUCCESS(RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) +
                                  ", '" + payload + "');"));
  }
  EXPECT_SUCCESS(RunSql(db, "DELETE FROM t WHERE k < 30;"));
  db.EmulateCrash();
  CorruptLastLivePages(prefix_ + ".db", 3);
  // Must reopen at all: a rejected open, or an empty catalog behind it, is what
  // a torn-but-repaired page used to look like from the outside.
  Database& reopened = Reopen();
  EXPECT_EQ(CountRows(reopened, "t"), size_t{210});
  // The catalog is still the original one: a recreated table is accepted only
  // because the dropped rows are gone, and the old data is still readable.
  EXPECT_EQ(SelectKeys(reopened, "t").size(), size_t{210});
  EXPECT_EQ(std::ranges::count(SelectKeys(reopened, "t"), int64_t{239}), 1);
  EXPECT_SUCCESS(
      RunSql(reopened, "INSERT INTO t VALUES (999, 'after-recovery');"));
  EXPECT_EQ(CountRows(reopened, "t"), size_t{211});
}

// RunSql without the ADD_FAILURE side effect: for call sites that branch on
// the status themselves (the known-wedge quarantine below).
namespace {
Status TrySql(Database& db, const std::string& sql) {
  SqlEngine engine(db);
  TransactionContext ctx = db.BeginContext();
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  if (!prepared.HasValue()) {
    return prepared.GetStatus();
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  Status stream_status = prepared.Value()->GetStatus();
  if (stream_status != Status::kSuccess) {
    static_cast<void>(ctx.txn_.Abort());
    return stream_status;
  }
  return ctx.txn_.PreCommit();
}
}  // namespace

// Runs `sql` inside `ctx` without committing: the statement's WAL records
// join whatever transaction `ctx` already holds.
namespace {

Status ExecInContext(Database& db, TransactionContext& ctx,
                     const std::string& sql) {
  SqlEngine engine(db);
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  if (!prepared.HasValue()) {
    ADD_FAILURE() << sql << "\n" << engine.LastError();
    return prepared.GetStatus();
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  return Status::kSuccess;
}
}  // namespace

// Regression guard for the fuzz-found nested-crash shape
// (session_regressions/sql_session_fuzz-repro-6000602889109468625): a loser
// transaction holding a CREATE INDEX (kSystemAllocPage records + index
// inserts) is interrupted by a crash; the restart loser-undo must compensate
// every allocation so the reclaimed page ids can be re-issued to later
// transactions under a DIFFERENT type. A following crash then replays
// ordinary row ops over pages whose incarnation the loser briefly changed --
// a stale redo (type mismatch) or a skipped undo surfaces here as a failed
// reopen with "WAL operation page type does not match target".
TEST_F(RecoveryExtraTest, LoserAllocationUndoDoesNotFreeRecycledPage) {
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64, v VARCHAR);"));
  const std::string payload = BigPayload();
  // Fill past the first row page so the loser below must allocate a fresh
  // page id for the table to keep growing.
  for (int i = 0; i < 40; ++i) {
    EXPECT_SUCCESS(RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) +
                                  ", '" + payload + "');"));
  }

  // Crash #1: an uncommitted multi-page insert dies without commit or
  // abort. Recovery #1 walks its chain as a loser and appends CLRs (a
  // runtime abort never runs, so no kCommit terminator is ever written and
  // EVERY later recovery re-walks this chain).
  {
    TransactionContext ctx = db.BeginContext();
    for (int i = 100; i < 140; ++i) {
      EXPECT_SUCCESS(ExecInContext(db, ctx,
                                   "INSERT INTO t VALUES (" +
                                       std::to_string(i) + ", '" + payload +
                                       "');"));
    }
  }
  db.EmulateCrash();
  Database& second = Reopen();
  EXPECT_EQ(CountRows(second), size_t{40});

  // No checkpoint here: the loser chain keeps re-walking on every restart.
  // A committed transaction now re-allocates the page id recovery #1 freed
  // (it sits at the free-list head) and fills it with real rows.
  for (int i = 40; i < 90; ++i) {
    EXPECT_SUCCESS(RunSql(second, "INSERT INTO t VALUES (" + std::to_string(i) +
                                      ", '" + payload + "');"));
  }

  // Crash #2: without compensation evidence the re-walk re-applies the
  // loser allocation's undo onto the recycled page -- the committed page
  // was re-initialized as free and the next full scan walked into it.
  db_->EmulateCrash();
  Database& third = Reopen();
  EXPECT_EQ(CountRows(third), size_t{90});
  const std::vector<int64_t> keys = SelectKeys(third);
  ASSERT_EQ(keys.size(), size_t{90});
  EXPECT_EQ(std::ranges::count(keys, int64_t{89}), 1);

  // The chain re-walks once more; the evidence guard must hold across
  // repeated recoveries, not just one.
  db_->EmulateCrash();
  Database& fourth = Reopen();
  EXPECT_EQ(CountRows(fourth), size_t{90});
}

// Overwrites the body of every branch-typed page image in the data file so
// the next open must rebuild them through the batch SPR (type field: the
// 8-byte slot at header offset magic+version+pid+lsn+rec_lsn = 32;
// kBranchPage == 5, native endian).
namespace {
void CorruptBranchPages(const std::string& db_file) {
  std::fstream file(db_file, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_FALSE(file.fail());
  file.seekg(0, std::ios_base::end);
  const auto file_size = file.tellg();
  const size_t page_count =
      static_cast<size_t>(file_size) / static_cast<size_t>(kPageSize);
  size_t corrupted = 0;
  for (size_t page_id = 1; page_id < page_count; ++page_id) {
    std::vector<char> image(kPageSize);
    file.seekg(static_cast<std::streamoff>(page_id) *
                   static_cast<std::streamoff>(kPageSize),
               std::ios_base::beg);
    file.read(image.data(), static_cast<std::streamsize>(image.size()));
    if (file.gcount() != static_cast<std::streamsize>(kPageSize)) {
      break;
    }
    uint64_t type = 0;
    DeserializeU64(image.data() + 32, &type);  // on-disk headers are BE
    if (type != static_cast<uint64_t>(PageType::kBranchPage)) {
      continue;
    }
    for (size_t offset = kPageHeaderSize; offset < kPageSize; ++offset) {
      image[offset] = static_cast<char>(0x5A ^ offset);
    }
    file.seekp(static_cast<std::streamoff>(page_id) *
                   static_cast<std::streamoff>(kPageSize),
               std::ios_base::beg);
    file.write(image.data(), static_cast<std::streamsize>(image.size()));
    ++corrupted;
  }
  ASSERT_GT(corrupted, size_t{0}) << "no branch page found to corrupt";
}

void RemoveDbArtifacts(const std::string& prefix) {
  std::error_code ec;
  std::filesystem::remove(prefix + ".db", ec);
  std::filesystem::remove(prefix + ".log.meta", ec);
  std::filesystem::remove(prefix + ".last_checkpoint", ec);
  std::filesystem::remove(prefix + ".last_checkpoint.tmp", ec);
  for (const uint64_t index : ExistingSegments(prefix)) {
    std::filesystem::remove(Logger::SegmentPath(prefix + ".log", index), ec);
  }
  std::filesystem::remove(prefix + ".log", ec);
}
}  // namespace

// Soak-found stale-separator root cause, reproduced deterministically.
//
// One uncommitted transaction grows an index tree by itself: leaf inserts,
// foster splits (kInsertBranch separators into the root), and the in-place
// root growth (kSystemAllocPage kInPlace -- its undo payload is the entire
// pre-growth body). Recovery pass 1 walks the loser chain newest-first
// (delete separators, free split pages, then undo the in-place growth with a
// BODY-RESTORING kCompensateAllocPage CLR) but stops after N applications --
// the TINYLAMB_TEST_UNDO_STOP_AFTER hook stands in for a kill -9 during the
// walk. Pass 2 finds the tree's branch pages torn, so the batch SPR anchors
// the root's rebuild at that body-restoring CLR and resurrects pre-CLR
// content: separators that point at children pass 1 already freed. Before the
// fix, undo evidence older than the anchor suppressed the re-delete and every
// stop point left live separators routing into free pages ("descent routed to
// a non-branch page" / the soak's FreePage FATAL). The fix voids that
// evidence (LogUndo's per-record type check keeps recycled incarnations safe)
// and a post-undo separator sweep removes anything that still routes into a
// freed page.
TEST_F(RecoveryExtraTest,
       PartialLoserUndoThenAnchorRestoreKeepsIndexConsistent) {
  constexpr const char* kStopEnv = "TINYLAMB_TEST_UNDO_STOP_AFTER";
  constexpr const char* kEorEnv = "TINYLAMB_DISABLE_EOR_CHECKPOINT";
  const std::string payload = BigPayload();
  for (const int stop_after : {1, 2, 3, 5, 8}) {
    const std::string prefix = "recovery_extra_test-sep-" + RandomString();
    {
      ScopedEnv stop_env(kStopEnv, std::to_string(stop_after));
      ScopedEnv eor_env(kEorEnv, "1");
      StatusOr<std::unique_ptr<Database>> created = Database::Create(prefix);
      ASSERT_SUCCESS(created.GetStatus());
      std::unique_ptr<Database> db = created.MoveValue();
      EXPECT_SUCCESS(RunSql(*db, "CREATE TABLE t (k VARCHAR, v INT64);"));
      {
        TransactionContext ctx = db->BeginContext();
        const IndexSchema schema("idx", {0}, {}, IndexMode::kNonUnique);
        EXPECT_SUCCESS(db->CreateIndex(ctx, "t", schema));
        ASSERT_SUCCESS(ctx.PreCommit());
      }
      // Committed base rows: the index root starts as a single leaf.
      EXPECT_SUCCESS(RunSql(*db, "INSERT INTO t VALUES ('base-one', 1);"));
      EXPECT_SUCCESS(RunSql(*db, "INSERT INTO t VALUES ('base-two', 2);"));

      // The loser: fat keys (~1.5 KiB) split the index after ~20 rows, so
      // this one uncommitted transaction grows the root in place
      // (kSystemAllocPage kInPlace), foster-splits leaves (kInsertBranch
      // separators into the root) and allocates fresh leaf pages; the final
      // DELETE then runs the merge/collapse machinery over the same tree.
      // The context is deliberately leaked: a destroyed Transaction aborts
      // in its destructor, and this shape needs the chain to stay
      // uncompensated exactly like a kill -9 leaves it.
      auto* leaked_ctx = new TransactionContext(db->BeginContext());
      for (int i = 100; i < 320; ++i) {
        EXPECT_SUCCESS(ExecInContext(*db, *leaked_ctx,
                                     "INSERT INTO t VALUES ('loser-key-" +
                                         std::to_string(i) + payload + "', " +
                                         std::to_string(i) + ");"));
      }
      // Flush the loser's pages (branch root image reaches the file for the
      // corruption step below) WITHOUT ending the transaction. The
      // checkpoint's FPW sits below pass 1's body-restoring CLR, so the
      // anchor selection is unaffected.
      ASSERT_SUCCESS(db->WriteCheckpoint().GetStatus());
      EXPECT_SUCCESS(ExecInContext(*db, *leaked_ctx, "DELETE FROM t;"));
      // Crash: drop the dirty pages (no write-back), then let the dtor flush
      // the WAL ring so the loser chain and pass 1's CLRs survive.
      db->EmulateCrash();
    }

    // Pass 1: recovery stops mid-undo at the hook (its CLRs are in the WAL;
    // the EOR checkpoint is disabled so nothing flushes above the walk).
    {
      ScopedEnv stop_env(kStopEnv, std::to_string(stop_after));
      ScopedEnv eor_env(kEorEnv, "1");
      StatusOr<std::unique_ptr<Database>> pass1 = Database::Create(prefix);
      ASSERT_SUCCESS(pass1.GetStatus()) << "stop_after=" << stop_after;
      pass1.MoveValue()->EmulateCrash();
    }

    // Pass 2: tear every branch page so the root rebuild anchors at pass 1's
    // body-restoring CLR, then recover fully.
    CorruptBranchPages(prefix + ".db");
    {
      ScopedEnv eor_env(kEorEnv, "1");
      StatusOr<std::unique_ptr<Database>> pass2 = Database::Create(prefix);
      ASSERT_SUCCESS(pass2.GetStatus())
          << "stop_after=" << stop_after << ": reopen failed";
      std::unique_ptr<Database> db2 = pass2.MoveValue();
      // The committed rows must be back (the loser's deletes were undone)
      // and every index route must resolve.
      EXPECT_EQ(CountRows(*db2), size_t{2}) << "stop_after=" << stop_after;
      // Reads must see the recovered state: the loser's deletes undone, the
      // committed rows present, and every index route resolving.
      EXPECT_SUCCESS(RunSql(*db2, "SELECT v FROM t WHERE k = 'base-one';"))
          << "stop_after=" << stop_after;
      EXPECT_SUCCESS(RunSql(*db2, "SELECT v FROM t ORDER BY k;"))
          << "stop_after=" << stop_after;
      EXPECT_EQ(CountRows(*db2), size_t{2}) << "stop_after=" << stop_after;
      // A fresh insert must land in the index without routing into a freed
      // page. This used to wedge on a same-thread page-latch double-take
      // ("failed to latch page 6: Resource deadlock avoided"): the stale
      // foster left on the rebuilt root named a page the same recovery had
      // freed, and the height growth re-issued it as the grow sibling. The
      // foster sweep in RecoverFrom (and the descent-side absorb) root it
      // out; this is the regression pin for both halves.
      EXPECT_SUCCESS(TrySql(*db2, "INSERT INTO t VALUES ('after', 500);"))
          << "stop_after=" << stop_after;
      EXPECT_EQ(CountRows(*db2), size_t{3}) << "stop_after=" << stop_after;
      db2->EmulateCrash();
    }

    // Pass 3: pass 2's CLRs + sweep must replay idempotently.
    {
      StatusOr<std::unique_ptr<Database>> pass3 = Database::Create(prefix);
      ASSERT_SUCCESS(pass3.GetStatus()) << "stop_after=" << stop_after;
      std::unique_ptr<Database> db3 = pass3.MoveValue();
      EXPECT_GE(CountRows(*db3), size_t{2}) << "stop_after=" << stop_after;
      EXPECT_SUCCESS(RunSql(*db3, "SELECT v FROM t ORDER BY k;"))
          << "stop_after=" << stop_after;
    }
    if (std::getenv("TINYLAMB_KEEP_TEST_DATABASES") == nullptr) {
      RemoveDbArtifacts(prefix);
    }
  }
}

// The soak finding behind the round-12 ghost-entry hunt: a committed
// transaction whose records interleave with a loser's structural growth
// (splits moving entries across pages) must keep EVERY effect across
// repeated interrupted recoveries -- not just row counts. The orders-row-
// survives-while-the-district-update-vanishes shape needs exactly this
// interleave, so the committed txn runs INSIDE the loser's open window
// here, and the verification checks updated values (not counts) on every
// pass.
TEST_F(RecoveryExtraTest, CommittedValuesSurviveRepeatedInterruptedRecovery) {
  constexpr const char* kStopEnv = "TINYLAMB_TEST_UNDO_STOP_AFTER";
  constexpr const char* kEorEnv = "TINYLAMB_DISABLE_EOR_CHECKPOINT";
  const std::string payload = BigPayload();
  for (const int stop_after : {1, 2, 3, 5, 8}) {
    const std::string prefix = "recovery_extra_test-val-" + RandomString();
    {
      ScopedEnv stop_env(kStopEnv, std::to_string(stop_after));
      ScopedEnv eor_env(kEorEnv, "1");
      StatusOr<std::unique_ptr<Database>> created = Database::Create(prefix);
      ASSERT_SUCCESS(created.GetStatus());
      std::unique_ptr<Database> db = created.MoveValue();
      EXPECT_SUCCESS(RunSql(*db, "CREATE TABLE t (k VARCHAR, v INT64);"));
      {
        TransactionContext ctx = db->BeginContext();
        const IndexSchema schema("idx", {0}, {}, IndexMode::kNonUnique);
        EXPECT_SUCCESS(db->CreateIndex(ctx, "t", schema));
        ASSERT_SUCCESS(ctx.PreCommit());
      }
      for (int i = 0; i < 40; ++i) {
        EXPECT_SUCCESS(RunSql(*db, "INSERT INTO t VALUES ('base-" +
                                       std::to_string(i) + "', " +
                                       std::to_string(i) + ");"));
      }

      // The loser grows the index through several splits. Mid-window, a
      // committed transaction interleaves its own insert and update on the
      // same table -- the WAL mixing that recovery must disentangle.
      auto* leaked_ctx = new TransactionContext(db->BeginContext());
      for (int i = 100; i < 160; ++i) {
        EXPECT_SUCCESS(ExecInContext(*db, *leaked_ctx,
                                     "INSERT INTO t VALUES ('loser-key-" +
                                         std::to_string(i) + payload + "', " +
                                         std::to_string(i) + ");"));
      }
      EXPECT_SUCCESS(RunSql(*db, "INSERT INTO t VALUES ('committed-new', 7);"));
      EXPECT_SUCCESS(RunSql(*db, "UPDATE t SET v = 99 WHERE k = 'base-5';"));
      for (int i = 160; i < 260; ++i) {
        EXPECT_SUCCESS(ExecInContext(*db, *leaked_ctx,
                                     "INSERT INTO t VALUES ('loser-key-" +
                                         std::to_string(i) + payload + "', " +
                                         std::to_string(i) + ");"));
      }
      // Flush the interleave to the file (FPW base for the corruption step)
      // without ending the loser.
      ASSERT_SUCCESS(db->WriteCheckpoint().GetStatus());
      EXPECT_SUCCESS(ExecInContext(*db, *leaked_ctx, "DELETE FROM t;"));
      db->EmulateCrash();
    }

    const auto verify = [&](Database& db, int pass) {
      EXPECT_EQ(CountRows(db), size_t{41})
          << "pass " << pass << " stop_after=" << stop_after;
      const StatusOr<int64_t> updated =
          SelectScalar(db, "SELECT v FROM t WHERE k = 'base-5';");
      ASSERT_SUCCESS(updated.GetStatus())
          << "committed update lost: pass " << pass
          << " stop_after=" << stop_after;
      EXPECT_EQ(updated.Value(), 99)
          << "pass " << pass << " stop_after=" << stop_after;
      const StatusOr<int64_t> inserted =
          SelectScalar(db, "SELECT v FROM t WHERE k = 'committed-new';");
      ASSERT_SUCCESS(inserted.GetStatus())
          << "committed insert lost: pass " << pass
          << " stop_after=" << stop_after;
      EXPECT_EQ(inserted.Value(), 7)
          << "pass " << pass << " stop_after=" << stop_after;
      // Every index route must resolve onto the recovered rows.
      EXPECT_SUCCESS(RunSql(db, "SELECT v FROM t WHERE k = 'base-39';"))
          << "pass " << pass << " stop_after=" << stop_after;
      EXPECT_SUCCESS(RunSql(db, "SELECT v FROM t ORDER BY k;"))
          << "pass " << pass << " stop_after=" << stop_after;
    };

    // Pass 1: recovery stops mid-undo; its CLRs stay in the WAL.
    {
      ScopedEnv stop_env(kStopEnv, std::to_string(stop_after));
      ScopedEnv eor_env(kEorEnv, "1");
      StatusOr<std::unique_ptr<Database>> pass1 = Database::Create(prefix);
      ASSERT_SUCCESS(pass1.GetStatus()) << "stop_after=" << stop_after;
      pass1.MoveValue()->EmulateCrash();
    }

    // Pass 2: tear every branch page (SPR anchor interplay), recover fully,
    // and verify the committed effects -- the ghost/loss shape fires here.
    CorruptBranchPages(prefix + ".db");
    {
      ScopedEnv eor_env(kEorEnv, "1");
      StatusOr<std::unique_ptr<Database>> pass2 = Database::Create(prefix);
      ASSERT_SUCCESS(pass2.GetStatus())
          << "stop_after=" << stop_after << ": reopen failed";
      std::unique_ptr<Database> db2 = pass2.MoveValue();
      verify(*db2, 2);
      db2->EmulateCrash();
    }

    // Pass 3: pass 2's CLRs + sweeps must replay idempotently.
    {
      StatusOr<std::unique_ptr<Database>> pass3 = Database::Create(prefix);
      ASSERT_SUCCESS(pass3.GetStatus()) << "stop_after=" << stop_after;
      std::unique_ptr<Database> db3 = pass3.MoveValue();
      verify(*db3, 3);
      db3->EmulateCrash();
    }
    // Pass 4: one more full cycle with NO corruption in between -- the
    // clean-restart twin of the soak's "second verification onwards".
    {
      StatusOr<std::unique_ptr<Database>> pass4 = Database::Create(prefix);
      ASSERT_SUCCESS(pass4.GetStatus()) << "stop_after=" << stop_after;
      std::unique_ptr<Database> db4 = pass4.MoveValue();
      verify(*db4, 4);
    }
    if (std::getenv("TINYLAMB_KEEP_TEST_DATABASES") == nullptr) {
      RemoveDbArtifacts(prefix);
    }
  }
}

TEST_F(RecoveryExtraTest, LoserDdlAllocationSurvivesNestedCrashes) {
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64, v VARCHAR);"));
  const std::string payload = BigPayload();
  for (int i = 0; i < 40; ++i) {
    EXPECT_SUCCESS(RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) +
                                  ", '" + payload + "');"));
  }
  ASSERT_SUCCESS(db.WriteCheckpoint().GetStatus());

  // Crash #2 of the fuzz trace: one uncommitted transaction holds a row
  // insert AND the index build (fresh page allocations + leaf/branch ops).
  // The context is destroyed without commit or abort, so its undo chain
  // stays uncompensated -- exactly what a crash-interrupted transaction
  // looks like to the next recovery.
  {
    TransactionContext ctx = db.BeginContext();
    EXPECT_SUCCESS(
        ExecInContext(db, ctx, "INSERT INTO t VALUES (100, 'loser');"));
    const IndexSchema schema("idx0", {1}, {}, IndexMode::kNonUnique);
    EXPECT_SUCCESS(db.CreateIndex(ctx, "t", schema));
  }
  db.EmulateCrash();

  // Recovery #2 must undo the loser and reopen cleanly.
  Database& second = Reopen();
  EXPECT_EQ(CountRows(second), size_t{40});

  // Reissue the reclaimed ids: rebuild the same index (committed this time)
  // and force fresh row-page allocations with fat rows. These are the
  // records crash #3's redo replays over the loser allocation's pages.
  for (int i = 40; i < 90; ++i) {
    EXPECT_SUCCESS(RunSql(second, "INSERT INTO t VALUES (" + std::to_string(i) +
                                      ", '" + payload + "');"));
  }
  EXPECT_SUCCESS(RunSql(second, "DELETE FROM t WHERE k = 100;"));
  {
    TransactionContext ctx = second.BeginContext();
    const IndexSchema schema("idx0", {1}, {}, IndexMode::kNonUnique);
    EXPECT_SUCCESS(second.CreateIndex(ctx, "t", schema));
    EXPECT_SUCCESS(ctx.txn_.PreCommit());
  }
  ASSERT_SUCCESS(second.WriteCheckpoint().GetStatus());

  // Crash #3: the reopen that used to fail with a page-type mismatch.
  db_->EmulateCrash();
  Database& third = Reopen();
  EXPECT_EQ(CountRows(third), size_t{90});
  const std::vector<int64_t> keys = SelectKeys(third);
  EXPECT_EQ(keys.size(), size_t{90});
  EXPECT_EQ(std::ranges::count(keys, int64_t{89}), 1);
  EXPECT_SUCCESS(RunSql(third, "INSERT INTO t VALUES (200, 'after');"));
  EXPECT_EQ(CountRows(third), size_t{91});
}

// Regression guard for restart + table growth: the in-memory table-tail
// cache is seeded from the catalog's `last_pid_` hint, which only DDL
// persists, so after ANY restart it can point behind the physical chain
// tail. ReserveTableTail's expected-match guard then rejected every attempt
// to append a new page with kConflicts, and the table could never grow
// again for the life of the process (surfaced by the fuzz repro
// sql_session_fuzz-repro-6000602889109468625 as lost committed rows).
// The tail cache must heal while the insert walk follows committed
// next-links (PageManager::AdvanceTableTail).
TEST_F(RecoveryExtraTest, RestartedTableGrowsAgain) {
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64, v VARCHAR);"));
  const std::string payload = BigPayload();
  // Fill the first row page and rotate onto a second one so the catalog's
  // last_pid_ (still the CREATE TABLE value) falls behind the real chain.
  for (int i = 0; i < 40; ++i) {
    EXPECT_SUCCESS(RunSql(db, "INSERT INTO t VALUES (" + std::to_string(i) +
                                  ", '" + payload + "');"));
  }
  ASSERT_SUCCESS(db.WriteCheckpoint().GetStatus());

  // Clean restart: every transaction committed, nothing to undo.
  db.EmulateCrash();
  Database& reopened = Reopen();
  EXPECT_EQ(CountRows(reopened), size_t{40});
  // These inserts must rotate onto fresh pages.
  for (int i = 40; i < 90; ++i) {
    EXPECT_SUCCESS(RunSql(reopened, "INSERT INTO t VALUES (" +
                                        std::to_string(i) + ", '" + payload +
                                        "');"));
  }
  EXPECT_EQ(CountRows(reopened), size_t{90});

  // And growth keeps working across a second restart.
  reopened.EmulateCrash();
  Database& again = Reopen();
  for (int i = 90; i < 120; ++i) {
    EXPECT_SUCCESS(RunSql(again, "INSERT INTO t VALUES (" + std::to_string(i) +
                                     ", '" + payload + "');"));
  }
  EXPECT_EQ(CountRows(again), size_t{120});
  EXPECT_EQ(std::ranges::count(SelectKeys(again), int64_t{119}), 1);
}

TEST_F(RecoveryExtraTest, CrashRecoveryPublishesMasterRecordWithoutTimer) {
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t (k INT64, v VARCHAR);"));
  EXPECT_SUCCESS(RunSql(db, "INSERT INTO t VALUES (1, 'first');"));
  // No periodic checkpoint has had time to run and none was requested
  // explicitly: a fresh database has no master record yet.
  EXPECT_FALSE(std::filesystem::exists(prefix_ + ".last_checkpoint"));
  db.EmulateCrash();
  Database& reopened = Reopen();
  EXPECT_EQ(CountRows(reopened), size_t{1});
  // End-of-recovery checkpoint: the replayed state is anchored by a master
  // record as soon as the open finishes, so a crash right after a long
  // recovery never re-pays the whole replay, and the WAL below the anchor
  // becomes recyclable without waiting for the periodic timer.
  ASSERT_TRUE(std::filesystem::exists(prefix_ + ".last_checkpoint"));
  const lsn_t anchored = MasterRecordLsn(prefix_ + ".last_checkpoint");
  EXPECT_GT(anchored, lsn_t{0});
  RecoveryManager recovery(prefix_ + ".log", nullptr);
  LogRecord probe;
  ASSERT_TRUE(recovery.ReadLog(anchored, &probe))
      << "master record points at " << anchored;
  EXPECT_EQ(probe.type, LogType::kBeginCheckpoint);

  // The anchor survives a second crash: recovery now starts at the published
  // checkpoint and the data is still there.
  reopened.EmulateCrash();
  Database& again = Reopen();
  EXPECT_EQ(CountRows(again), size_t{1});
}

TEST_F(RecoveryExtraTest, AbortedRecycledAllocationUndoSurvivesRestart) {
  // DDL churn builds a free list; crashes with DDL in flight make recovery
  // re-derive the allocator high-water from partial WAL evidence. The next
  // transaction that allocates a RECYCLED page and then aborts walks the
  // allocation's undo, whose payload self-check (recycled_next must stay
  // within the restored high-water) rejects an inconsistent restore. The
  // exact free-page-above-high-water shape that produced this in the wild
  // (session fuzzer seed 29) is exercised by the SeededSessionsHoldState
  // sweep; this test pins the recurring restart+DDL-churn+abort pattern.
  ScopedEnv segment_env(kSegmentEnv, std::to_string(kTinySegmentSize));
  Database& db = Reopen();
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t1 (k INT64, v VARCHAR);"));
  EXPECT_SUCCESS(RunSql(db, "CREATE TABLE t2 (k INT64, v VARCHAR);"));
  const std::string payload = BigPayload();
  for (int i = 0; i < 12; ++i) {
    EXPECT_SUCCESS(RunSql(db, "INSERT INTO t1 VALUES (" + std::to_string(i) +
                                  ", '" + payload + "');"));
    EXPECT_SUCCESS(RunSql(db, "INSERT INTO t2 VALUES (" + std::to_string(i) +
                                  ", '" + payload + "');"));
  }
  // Committed destroys leave real free pages behind.
  EXPECT_SUCCESS(RunSql(db, "DROP TABLE t2;"));
  ASSERT_SUCCESS(db.WriteCheckpoint().GetStatus());

  // Crash with a DDL transaction in flight: the loser's allocation evidence
  // is excluded from the high-water restore while its pages may survive as
  // free images.
  {
    SqlEngine engine(db);
    TransactionContext ctx = db.BeginContext();
    StatusOr<Executor> prepared =
        engine.Prepare(ctx, "CREATE TABLE loser_t (k INT64);");
    EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << engine.LastError();
    Row row;
    static_cast<void>(prepared.Value()->Next(&row, nullptr));
    // No commit: the context dies as a loser at the crash below.
  }
  Database& reopened = Reopen();
  EXPECT_EQ(CountRows(reopened, "t1"), size_t{12});

  // Recycled allocation + abort after restart, twice around another crash:
  // every abort must walk the recycled allocation's undo cleanly.
  for (int round = 0; round < 2; ++round) {
    Database& current = round == 0 ? reopened : Reopen();
    EXPECT_EQ(CountRows(current, "t1"), size_t{12}) << "round " << round;
    EXPECT_SUCCESS(RunSql(current, "CREATE TABLE churn" +
                                       std::to_string(round) +
                                       " (k INT64, v VARCHAR);"))
        << "round " << round;
    EXPECT_SUCCESS(RunSql(current, "INSERT INTO churn" + std::to_string(round) +
                                       " VALUES (1, 'x');"))
        << "round " << round;
    {
      SqlEngine engine(current);
      TransactionContext ctx = current.BeginContext();
      const std::string victim = "victim" + std::to_string(round);
      StatusOr<Executor> prepared = engine.Prepare(
          ctx, "CREATE TABLE " + victim + " (k INT64, v VARCHAR);");
      EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << engine.LastError();
      if (!prepared.HasValue()) {
        static_cast<void>(ctx.txn_.Abort());
        continue;
      }
      Row row;
      static_cast<void>(prepared.Value()->Next(&row, nullptr));
      StatusOr<Executor> insert =
          engine.Prepare(ctx, "INSERT INTO " + victim + " VALUES (9, 'y');");
      EXPECT_EQ(insert.GetStatus(), Status::kSuccess) << engine.LastError();
      if (insert.HasValue()) {
        static_cast<void>(insert.Value()->Next(&row, nullptr));
      }
      EXPECT_SUCCESS(ctx.txn_.Abort())
          << "round " << round << ": recycled-allocation abort failed";
    }
  }
  Database& final_db = Reopen();
  EXPECT_EQ(CountRows(final_db, "t1"), size_t{12});
  EXPECT_EQ(CountRows(final_db, "churn0"), size_t{1});
  EXPECT_EQ(CountRows(final_db, "churn1"), size_t{1});
}

TEST_F(RecoveryExtraTest, TornPageRebuildWithDirtyMetaPageNeverSelfDeadlocks) {
  ScopedEnv segment_env(kSegmentEnv, std::to_string(kTinySegmentSize));
  ScopedEnv pool_env(kPoolEnv, std::to_string(kTinyPoolBytes));
  for (int cycle = 0; cycle < 3; ++cycle) {
    const std::string name = "t" + std::to_string(cycle);
    Database& db = Reopen();
    EXPECT_SUCCESS(
        RunSql(db, "CREATE TABLE " + name + " (k INT64, v VARCHAR);"));
    const std::string payload = BigPayload();
    for (int i = 0; i < 150; ++i) {
      std::string sql = "INSERT INTO " + name;
      sql += " VALUES (";
      sql += std::to_string(i);
      sql += ", '";
      sql += payload;
      sql += "');";
      EXPECT_SUCCESS(RunSql(db, sql));
    }
    ASSERT_SUCCESS(db.WriteCheckpoint().GetStatus());
    for (int i = 150; i < 240; ++i) {
      std::string sql = "INSERT INTO " + name;
      sql += " VALUES (";
      sql += std::to_string(i);
      sql += ", '";
      sql += payload;
      sql += "');";
      EXPECT_SUCCESS(RunSql(db, sql));
    }
    EXPECT_SUCCESS(RunSql(db, "DELETE FROM " + name + " WHERE k < 30;"));
    EXPECT_EQ(CountRows(db, name), size_t{210});
    db.EmulateCrash();
    CorruptLastLivePages(prefix_ + ".db", 3);
    Database& reopened = Reopen();
    EXPECT_EQ(CountRows(reopened, name), size_t{210});
  }
}

}  // namespace tinylamb
