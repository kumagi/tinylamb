/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "logger.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "common/constants.hpp"
#include "common/platform/file_io.hpp"
#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "recovery/log_record.hpp"

namespace tinylamb {
class LoggerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string prefix = "logger_test-" + RandomString();
    log_name_ = prefix + ".log";
    l_ = Logger::Create(log_name_, 32, 1).MoveValue();
  }

  void TearDown() override {
    std::this_thread::sleep_for(std::chrono::microseconds(5));
    // Best-effort cleanup; a missing file must not fail the test.
    tinylamb::RemoveLogAndSegments(log_name_);
  }

  void WaitForCommit(lsn_t target_lsn, size_t timeout_ms = 10000) {
    size_t counter = 0;
    while (l_->CommittedLSN() != target_lsn && counter < timeout_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      ++counter;
    }
    EXPECT_LT(counter, timeout_ms);
  }

  std::string log_name_;
  std::unique_ptr<Logger> l_;
};

TEST_F(LoggerTest, CommitDrivenSyncKeepsRequestedBarriersImmediate) {
  l_->SetCommitDrivenSync(true);
  const auto target = l_->AddLog("requested").Value() + 9;
  WaitForCommit(target);
  // The short background timer must not start an avoidable pre-commit sync.
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_LT(l_->DurableLSN(), target);
  ASSERT_SUCCESS(l_->WaitForDurable(target));
  EXPECT_GE(l_->DurableLSN(), target);
  const auto tail = l_->AddLog("finish").Value() + 6;
  ASSERT_SUCCESS(l_->Finish());
  EXPECT_GE(l_->DurableLSN(), tail);
}

TEST_F(LoggerTest, CommitDrivenSyncStillHasBoundedBackgroundProgress) {
  l_->SetCommitDrivenSync(true);
  const auto target = l_->AddLog("background").Value() + 10;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (l_->DurableLSN() < target &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_GE(l_->DurableLSN(), target);
}

TEST_F(LoggerTest, Construct) {
  // Arrange -- nothing to set up; default Logger constructed by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(LoggerTest, AppendOne) {
  // Arrange -- one LogRecord (kBegin) and a fresh Logger with 32 KiB buffer
  LogRecord l(0xcafebabe, 0xdeadbeef, LogType::kBegin);

  // Act -- append the serialized log; wait for commit; read back via ifstream
  lsn_t lsn = l_->AddLog(l.Serialize()).Value();
  ASSERT_EQ(0, lsn);  // Inserted place must be the beginning of the log.
  WaitForCommit(0 + l.Size());
  EXPECT_EQ(std::filesystem::file_size(log_name_), l.Size());
  std::ifstream file;
  file.open(log_name_);
  std::string file_data;
  file >> file_data;

  // Assert -- file content equals the serialized LogRecord
  ASSERT_EQ(file_data, l.Serialize());
}

TEST_F(LoggerTest, AddLogCopiesStringViewBeforeReturning) {
  std::string payload = "original";
  const std::string expected = payload;
  const lsn_t lsn = l_->AddLog(payload).Value();
  payload.assign(payload.size(), 'x');
  WaitForCommit(lsn + expected.size());

  std::ifstream file(log_name_, std::ios::binary);
  std::string actual;
  file >> actual;
  EXPECT_EQ(actual, expected);
}

TEST_F(LoggerTest, AppendTwo) {
  // Arrange -- two deterministic strings d1/d2 of known lengths
  std::string d1("6uRa9BIQb5RD2p8dIxXKtpgIDU1HBT7wfqfdZDApAqX5crm36WaCgRXgQ");
  std::string d2("P16dKMXY5TvrZVU7bKqLuAdf636mxmSsZpaDkocoClSZs3pX3");

  // Act -- append d1, sleep 1us, append d2; wait for commit; read back via
  // ifstream
  l_->AddLog(d1);
  std::this_thread::sleep_for(std::chrono::microseconds(1));
  l_->AddLog(d2);
  WaitForCommit(d1.size() + d2.size());
  EXPECT_EQ(std::filesystem::file_size(log_name_), d1.size() + d2.size());
  std::ifstream file;
  file.open(log_name_);
  std::string file_data;
  file >> file_data;

  // Assert -- file content equals d1+d2 concatenated
  ASSERT_EQ(file_data, d1 + d2);
}

TEST_F(LoggerTest, AppendMany) {
  // Arrange -- 64 random strings of deterministic lengths (i*31 % 40 + 1)
  lsn_t lsn = 0;
  size_t size = 0;

  // Act -- append each random string; accumulate LSN and total size; wait for
  // commit; read back file size
  for (int i = 0; i < 64; ++i) {
    auto random_size = static_cast<size_t>(((i * 31) % 40) + 1);
    lsn = l_->AddLog(RandomString(random_size)).Value() + random_size;
    size += random_size;
    EXPECT_EQ(lsn, size);
  }
  WaitForCommit(lsn);

  // Assert -- committed LSN equals total size and file size matches
  EXPECT_EQ(std::filesystem::file_size(log_name_), size);
}

TEST_F(LoggerTest, AppendExponential) {
  // Arrange -- reset Logger to a fresh instance; prepare 1000 exponential-size
  // strings ('x' repeated).  A single unlimited segment keeps the whole
  // stream in the base file so the byte-count assertion below observes the
  // append path directly instead of segment rotation.
  l_ = Logger::Create(log_name_, size_t{1024} * 1024 * 8, 1, size_t{1} << 30)
           .MoveValue();
  lsn_t lsn = 0;
  size_t size = 0;

  // Act -- append each string; accumulate LSN and total size; wait for commit;
  // read back file size
  for (int i = 0; i < 1000; ++i) {
    std::string data(static_cast<size_t>((i * i) + 1), 'x');
    lsn = l_->AddLog(data).Value();
    EXPECT_EQ(lsn, size);
    size += static_cast<size_t>((i * i) + 1);
  }
  WaitForCommit(size);

  // Assert -- committed LSN equals total size and file size matches
  EXPECT_EQ(std::filesystem::file_size(log_name_), size);
}

TEST_F(LoggerTest, Verify) {
  // Arrange -- a 1024-byte random string as the log payload
  std::string written_log = RandomString(1024);

  // Act -- append the string; wait for commit; read back via ifstream
  l_->AddLog(written_log);
  WaitForCommit(written_log.size());
  std::ifstream file;
  file.open(log_name_);
  std::string file_data;
  file >> file_data;

  // Assert -- file content equals the written_log payload
  ASSERT_EQ(file_data, written_log);
}

TEST_F(LoggerTest, WaitForDurableObservesFsync) {
  std::string payload = RandomString(256);
  const lsn_t lsn = l_->AddLog(payload).Value();
  l_->WaitForDurable(lsn + payload.size());
  EXPECT_GE(l_->DurableLSN(), lsn + payload.size());
  EXPECT_GE(l_->CommittedLSN(), lsn + payload.size());
  EXPECT_GT(l_->GetSyncStats().count, 0U);
  EXPECT_GT(l_->GetSyncStats().nanoseconds, 0U);
}

TEST_F(LoggerTest, FinishDrainsWithoutSpin) {
  std::string payload(4096, 'z');
  l_->AddLog(payload);
  l_->Finish();
  EXPECT_EQ(l_->CommittedLSN(), payload.size());
  EXPECT_GE(l_->DurableLSN(), payload.size());
  l_.reset();
}

TEST_F(LoggerTest, WaitForDurableAfterFinishReturnsError) {
  ASSERT_SUCCESS(l_->Finish());
  EXPECT_EQ(l_->WaitForDurable(l_->DurableLSN() + 1), Status::kRuntimeError);
}

TEST_F(LoggerTest, ReopenExistingWalKeepsContent) {
  // Regression: the worker used to start before flushed_lsn_/buffered_lsn_
  // were seeded with the on-disk size, so reopening an existing WAL could
  // append zero-filled buffer bytes over the tail of the log.
  const std::string first(128, 'x');
  ASSERT_EQ(l_->AddLog(first).Value(), 0);
  l_->WaitForDurable(first.size());
  l_.reset();

  auto second = Logger::Create(log_name_, 32, 1).MoveValue();
  EXPECT_EQ(second->BufferedLSN(), first.size());
  const std::string appended(64, 'y');
  const lsn_t lsn = second->AddLog(appended).Value();
  EXPECT_EQ(lsn, first.size());
  second->WaitForDurable(first.size() + appended.size());
  EXPECT_EQ(std::filesystem::file_size(log_name_),
            first.size() + appended.size());

  // Act -- read back the whole file.
  std::ifstream file(log_name_, std::ios::binary);
  const std::string data{std::istreambuf_iterator<char>(file),
                         std::istreambuf_iterator<char>()};

  // Assert -- the previous WAL content is intact, no zeros were spliced in.
  EXPECT_EQ(data, first + appended);
}

TEST_F(LoggerTest, WriteErrorUnblocksAllWaiters) {
  // Arrange -- point the log fd at a write-failing descriptor (/dev/full on
  // Linux: ENOSPC) so every write() fails while the fd itself stays valid.
  const int full_fd = platform::OpenWriteFailingDescriptor();
  ASSERT_GE(full_fd, 0);
  ASSERT_EQ(platform::Dup2(full_fd, l_->Fd()), l_->Fd());
  ASSERT_EQ(platform::CloseFile(full_fd), 0);

  // Act -- enqueue data; the worker hits ENOSPC and must record the failure
  // instead of dying silently. The enqueue itself may observe the failure
  // before all bytes land, which must surface as an exception too; both
  // outcomes are acceptable here, the poll loop below asserts on Failed().
  bool enqueue_observed_failure = false;
  try {
    (void)l_->AddLog(std::string(64, 'a'));
  } catch (const std::runtime_error&) {
    enqueue_observed_failure = true;
  }
  (void)enqueue_observed_failure;
  bool failed = false;
  for (int i = 0; i < 10000 && !failed; ++i) {
    failed = l_->Failed();
    if (!failed) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  // Assert -- error state is observable and every waiter fails fast instead
  // of blocking forever.
  EXPECT_TRUE(failed);
  EXPECT_TRUE(l_->Failed());
  EXPECT_EQ(l_->ErrorNumber(), platform::kWriteFailingErrno);
  EXPECT_FALSE(l_->AddLog(std::string(16, 'b')).HasValue());
  EXPECT_NE(l_->WaitForDurable(l_->BufferedLSN() + 4096), Status::kSuccess);
  EXPECT_NE(l_->Finish(), Status::kSuccess);
}

TEST_F(LoggerTest, AddLogReturnsFreshLsnAfterBufferFullWait) {
  // Regression: a producer parked in the full-buffer wait used to return its
  // stale pre-wait LSN even though another producer had consumed that LSN
  // space, corrupting prev_lsn chains.
  constexpr size_t kBufSize = 512;
  l_.reset();
  auto logger = Logger::Create(log_name_, kBufSize, 1).MoveValue();

  // Arrange -- exactly fill the ring buffer so both producers must park.
  logger->AddLog(std::string(kBufSize, 'f'));
  lsn_t first_lsn = 0;
  lsn_t second_lsn = 0;

  // Act -- two concurrent producers race for the freed space.
  std::thread a(
      [&] { first_lsn = logger->AddLog(std::string(16, 'a')).Value(); });
  std::thread b(
      [&] { second_lsn = logger->AddLog(std::string(16, 'b')).Value(); });
  a.join();
  b.join();
  logger->WaitForDurable(kBufSize + 32);

  // Assert -- distinct, non-overlapping start positions covering
  // [kBufSize, kBufSize + 32); a stale LSN would collide with the sibling's.
  EXPECT_NE(first_lsn, second_lsn);
  EXPECT_GE(std::min(first_lsn, second_lsn), kBufSize);
  EXPECT_LE(std::max(first_lsn, second_lsn) + 16, kBufSize + 32);
  EXPECT_EQ(std::filesystem::file_size(log_name_), kBufSize + 32);
}

namespace {
// Self-framing test payload: magic | thread | seq | length | fill.
// A record fragmented by another producer breaks the magic/length/fill
// walk in D1NoRecordsInterleavedAcrossProducers.
constexpr uint32_t kD1Magic = 0x44315231U;  // "D1R1"

std::string MakeD1Payload(uint32_t thread, uint32_t seq, size_t body_size) {
  std::string out;
  out.reserve(16 + body_size);
  auto push32 = [&out](uint32_t v) {
    out.push_back(static_cast<char>((v >> 24) & 0xFF));
    out.push_back(static_cast<char>((v >> 16) & 0xFF));
    out.push_back(static_cast<char>((v >> 8) & 0xFF));
    out.push_back(static_cast<char>(v & 0xFF));
  };
  push32(kD1Magic);
  push32(thread);
  push32(seq);
  push32(static_cast<uint32_t>(body_size));
  out.append(body_size, static_cast<char>((thread * 31) + seq));
  return out;
}
}  // namespace

// D1 (docs/design.md) acceptance 1: payloads that cross buffer boundaries
// many times over must still parse back record-atomic from several producer
// threads.  Before D1 the full-buffer wait released the enqueue latch
// mid-record and the byte stream was fragmented (reproduced 8/8).
TEST_F(LoggerTest, D1NoRecordsInterleavedAcrossProducers) {
  constexpr size_t kBufSize = 4096;
  constexpr size_t kThreads = 8;
  constexpr size_t kRecordsPerThread = 8;
  l_.reset();
  auto logger = Logger::Create(log_name_, kBufSize, 1).MoveValue();

  std::vector<std::thread> producers;
  producers.reserve(kThreads);
  for (size_t t = 0; t < kThreads; ++t) {
    producers.emplace_back([&, t] {
      for (size_t s = 0; s < kRecordsPerThread; ++s) {
        // 16KB-class bodies against a 4KB ring: every record spans many
        // full-buffer waits and several wrap-around copies.
        const size_t body = (s % 2 == 0) ? 16384 : 64;
        (void)logger->AddLog(MakeD1Payload(static_cast<uint32_t>(t),
                                           static_cast<uint32_t>(s), body));
      }
    });
  }
  for (auto& th : producers) {
    th.join();
  }
  logger->Finish();

  // Assert -- sequential parse: magic, length and fill bytes must match for
  // every record; an interleaved fragment fails this walk immediately.
  std::ifstream file(log_name_, std::ios::binary);
  const auto read32 = [&file]() {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
      v = (v << 8) | static_cast<uint32_t>(file.get());
    }
    return v;
  };
  size_t records = 0;
  while (file.peek() != std::ifstream::traits_type::eof()) {
    const uint32_t magic = read32();
    ASSERT_EQ(magic, kD1Magic) << (records != 0U);
    const uint32_t thread = read32();
    const uint32_t seq = read32();
    const uint32_t length = read32();
    ASSERT_LT(thread, kThreads);
    ASSERT_LT(seq, kRecordsPerThread);
    const int fill = static_cast<unsigned char>((thread * 31) + seq);
    for (uint32_t i = 0; i < length; ++i) {
      ASSERT_EQ(file.get(), fill)
          << (records != 0U) << (thread != 0U) << (seq != 0U);
    }
    ++records;
  }
  EXPECT_EQ(records, kThreads * kRecordsPerThread);
}

TEST_F(LoggerTest, D1RejectsRecordOverMaxSize) {
  l_.reset();
  auto logger = Logger::Create(log_name_, 4096, 1).MoveValue();
  ASSERT_TRUE(logger != nullptr);
  EXPECT_FALSE(
      logger->AddLog(std::string(Logger::kMaxRecordSize + 1, 'x')).HasValue());
  EXPECT_TRUE(
      logger->AddLog(std::string(Logger::kMaxRecordSize, 'x')).HasValue());
  EXPECT_EQ(logger->Finish(), Status::kSuccess);
}

// RecycleUpTo must keep LowestRetainedLSN exact across repeated calls: the
// recycle cursor tracks the retained start so a long-running database pays
// only for its live segment window on every checkpoint, never for the total
// history it recycled away.
TEST_F(LoggerTest, RecycleUpToTracksLowestRetainedAcrossCalls) {
  constexpr size_t kSegmentSize = size_t{4} * 1024;
  const std::string name = "logger_test-recycle-" + RandomString() + ".log";
  auto logger = Logger::Create(name, 1024, 1, kSegmentSize).MoveValue();
  const std::string payload(256, 'x');
  for (int i = 0; i < 64; ++i) {
    ASSERT_TRUE(logger->AddLog(payload).HasValue());
  }
  ASSERT_SUCCESS(logger->WaitForDurable(logger->BufferedLSN()));
  ASSERT_GT(logger->BufferedLSN(), static_cast<lsn_t>(2) * kSegmentSize);
  EXPECT_EQ(logger->LowestRetainedLSN(), 0);

  EXPECT_EQ(logger->RecycleUpTo(2 * kSegmentSize), 2 * lsn_t{kSegmentSize});
  EXPECT_EQ(logger->LowestRetainedLSN(), 2 * lsn_t{kSegmentSize});
  // Idempotent: a second pass at the same floor recycles nothing and keeps
  // the retained start.
  EXPECT_EQ(logger->RecycleUpTo(2 * kSegmentSize), 0);
  EXPECT_EQ(logger->LowestRetainedLSN(), 2 * lsn_t{kSegmentSize});

  EXPECT_EQ(logger->RecycleUpTo(3 * kSegmentSize), 3 * lsn_t{kSegmentSize});
  EXPECT_EQ(logger->LowestRetainedLSN(), 3 * kSegmentSize);

  logger.reset();
  RemoveLogAndSegments(name);
}

// TryCopyBuffered serves exactly the bytes this process appended and that
// the ring still holds: nothing below the open-time WAL end (the ring starts
// zeroed on a reopen), nothing overwritten by the wrap, nothing past the
// buffered end.
TEST_F(LoggerTest, TryCopyBufferedServesResidentWindow) {
  constexpr size_t kBufSize = 1024;
  l_.reset();
  auto logger = Logger::Create(log_name_, kBufSize, 60'000).MoveValue();
  ASSERT_TRUE(logger != nullptr);
  const std::string first = "first-payload-0123456789";
  const std::string second = "second-payload-abcdefghijklmnop";
  const lsn_t first_lsn = logger->AddLog(first).Value();
  const lsn_t second_lsn = logger->AddLog(second).Value();
  const lsn_t end = second_lsn + second.size();

  auto read_back = [&](lsn_t at, size_t len) -> std::optional<std::string> {
    std::string out(len, '\0');
    if (!logger->TryCopyBuffered(at, out.data(), len)) {
      return std::nullopt;
    }
    return out;
  };

  // Both payloads are resident and byte-identical.
  EXPECT_EQ(read_back(first_lsn, first.size()), first);
  EXPECT_EQ(read_back(second_lsn, second.size()), second);
  // A read straddling the two payloads reads the contiguous stream.
  EXPECT_EQ(read_back(first_lsn, first.size() + second.size()), first + second);
  // Past the buffered end: not resident.
  EXPECT_FALSE(read_back(end, 1).has_value());
  EXPECT_FALSE(read_back(second_lsn, second.size() + 1).has_value());
  // Zero length is trivially complete.
  EXPECT_TRUE(logger->TryCopyBuffered(first_lsn, nullptr, 0));

  // Fill past the ring capacity: the first payload is overwritten and must
  // no longer be served, while the newest bytes stay exact.
  const std::string filler(kBufSize * 2, 'f');
  const lsn_t filler_lsn = logger->AddLog(filler).Value();
  EXPECT_FALSE(read_back(first_lsn, first.size()).has_value());
  const std::string tail = filler.substr(filler.size() - 32);
  const lsn_t tail_lsn = filler_lsn + filler.size() - 32;
  EXPECT_EQ(read_back(tail_lsn, 32), tail);

  EXPECT_EQ(logger->Finish(), Status::kSuccess);
}

// A logger reopened over an existing WAL serves nothing from its zeroed
// ring: every byte below the open-time end lives only in the segment files.
TEST_F(LoggerTest, TryCopyBufferedNeverServesPreopenBytes) {
  l_.reset();
  {
    auto first = Logger::Create(log_name_, 512, 60'000).MoveValue();
    ASSERT_TRUE(first != nullptr);
    first->AddLog("seeded bytes below the reopen point");
    ASSERT_SUCCESS(first->WaitForDurable(first->BufferedLSN()));
  }
  auto reopened = Logger::Create(log_name_, 512, 60'000).MoveValue();
  ASSERT_TRUE(reopened != nullptr);
  const lsn_t seeded_end = reopened->BufferedLSN();
  EXPECT_GT(seeded_end, lsn_t{0});
  std::string out(8, '\0');
  EXPECT_FALSE(reopened->TryCopyBuffered(0, out.data(), out.size()));
  EXPECT_FALSE(
      reopened->TryCopyBuffered(seeded_end - 8, out.data(), out.size()));
  // New appends are served.
  const std::string fresh = "fresh-append";
  const lsn_t fresh_lsn = reopened->AddLog(fresh).Value();
  EXPECT_EQ(fresh_lsn, seeded_end);
  std::string served(fresh.size(), '\0');
  EXPECT_TRUE(
      reopened->TryCopyBuffered(fresh_lsn, served.data(), served.size()));
  EXPECT_EQ(served, fresh);
  EXPECT_EQ(reopened->Finish(), Status::kSuccess);
}

TEST_F(LoggerTest, FixedSegmentsRetainLogicalEndAndPersistedGeometry) {
  l_.reset();
  RemoveLogAndSegments(log_name_);
  constexpr size_t kSegment = 4096;
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Logger>, fixed,
                        Logger::Create(log_name_, 32, 60'000, kSegment, true));
  ASSERT_TRUE(fixed->FixedSizeSegments());
  EXPECT_EQ(std::filesystem::file_size(log_name_), kSegment + 64);
  ASSERT_SUCCESS_AND_EQ(Logger::SegmentLogicalSize(log_name_, 0, kSegment), 0);
  ASSERT_SUCCESS_AND_EQ(fixed->AddLog("durable-prefix"), 0);
  ASSERT_SUCCESS(fixed->WaitForDurable(14));
  ASSERT_SUCCESS_AND_EQ(Logger::SegmentLogicalSize(log_name_, 0, kSegment), 14);
  ASSERT_SUCCESS(fixed->Finish());
  fixed.reset();
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Logger>, reopened,
                        Logger::Create(log_name_, 32, 60'000, kSegment / 2));
  EXPECT_TRUE(reopened->FixedSizeSegments());
  EXPECT_EQ(reopened->SegmentSize(), kSegment);
  EXPECT_EQ(reopened->DurableLSN(), 14);
  ASSERT_SUCCESS_AND_EQ(reopened->AddLog("-more"), 14);
  ASSERT_SUCCESS(reopened->WaitForDurable(19));
  ASSERT_SUCCESS(reopened->Finish());
  std::ifstream input(log_name_, std::ios::binary);
  std::string prefix(19, '\0');
  input.read(prefix.data(), prefix.size());
  EXPECT_EQ(prefix, "durable-prefix-more");
  EXPECT_EQ(std::filesystem::file_size(log_name_), kSegment + 64);
}

TEST_F(LoggerTest, FixedSegmentsRollAndLogicalTruncateWithoutChangingSize) {
  l_.reset();
  RemoveLogAndSegments(log_name_);
  constexpr size_t kSegment = 128;
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Logger>, fixed,
                        Logger::Create(log_name_, 32, 60'000, kSegment, true));
  const std::string payload(100, 'x');
  ASSERT_SUCCESS_AND_EQ(fixed->AddLog(payload), 0);
  ASSERT_SUCCESS_AND_EQ(fixed->AddLog(payload), kSegment);
  ASSERT_SUCCESS_AND_EQ(fixed->AddLog(payload), 2 * kSegment);
  ASSERT_SUCCESS(fixed->WaitForDurable(fixed->BufferedLSN()));
  ASSERT_SUCCESS_AND_EQ(Logger::SegmentLogicalSize(log_name_, 0, kSegment),
                        100);
  ASSERT_SUCCESS_AND_EQ(Logger::SegmentLogicalSize(log_name_, 1, kSegment),
                        100);
  ASSERT_SUCCESS(fixed->TruncateTo(130));
  EXPECT_FALSE(std::filesystem::exists(Logger::SegmentPath(log_name_, 2)));
  EXPECT_EQ(std::filesystem::file_size(Logger::SegmentPath(log_name_, 1)),
            kSegment + 64);
  ASSERT_SUCCESS_AND_EQ(fixed->AddLog("replacement"), 130);
  ASSERT_SUCCESS(fixed->WaitForDurable(141));
  ASSERT_SUCCESS(fixed->Finish());
  fixed.reset();
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Logger>, reopened,
                        Logger::Create(log_name_, 32, 1, kSegment));
  EXPECT_EQ(reopened->BufferedLSN(), 141);
}

TEST_F(LoggerTest, PreallocationRequestNeverConvertsExistingRawSeries) {
  ASSERT_SUCCESS(l_->AddLog("legacy").GetStatus());
  ASSERT_SUCCESS(l_->Finish());
  l_.reset();
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Logger>, raw,
                        Logger::Create(log_name_, 32, 1, 4096, true));
  EXPECT_FALSE(raw->FixedSizeSegments());
  EXPECT_EQ(raw->BufferedLSN(), 6);
  ASSERT_SUCCESS(raw->Finish());
  EXPECT_EQ(std::filesystem::file_size(log_name_), 6);
}

TEST_F(LoggerTest, FixedSegmentsRejectEitherDamagedSlotAndForcedScanKeepsBody) {
  l_.reset();
  RemoveLogAndSegments(log_name_);
  constexpr size_t kSegment = 4096;
  for (const size_t damage_offset :
       {kSegment, kSegment + 32, kSegment + 28, kSegment + 60}) {
    RemoveLogAndSegments(log_name_);
    {
      ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Logger>, fixed,
                            Logger::Create(log_name_, 32, 1, kSegment, true));
      ASSERT_SUCCESS(fixed->AddLog("acknowledged").GetStatus());
      ASSERT_SUCCESS(fixed->WaitForDurable(fixed->BufferedLSN()));
      ASSERT_SUCCESS(fixed->Finish());
    }
    std::fstream frame(log_name_,
                       std::ios::binary | std::ios::in | std::ios::out);
    frame.seekg(damage_offset);
    char original = 0;
    frame.get(original);
    frame.seekp(damage_offset);
    frame.put(static_cast<char>(original ^ 0x5a));
    frame.close();
    EXPECT_FALSE(Logger::SegmentLogicalSize(log_name_, 0, kSegment).HasValue());
    EXPECT_FALSE(Logger::Create(log_name_, 32, 1, kSegment).HasValue());
    ASSIGN_OR_ASSERT_FAIL(
        std::unique_ptr<Logger>, forced,
        Logger::Create(log_name_, 32, 60'000, kSegment, false, true));
    // An older valid slot is not a safe prefix proof after corruption.
    EXPECT_EQ(forced->BufferedLSN(), kSegment);
    std::ifstream body(log_name_, std::ios::binary);
    std::string prefix(12, '\0');
    body.read(prefix.data(), prefix.size());
    EXPECT_EQ(prefix, "acknowledged");
    ASSERT_SUCCESS(forced->TruncateTo(12));
    ASSERT_SUCCESS(forced->Finish());
  }
}

TEST_F(LoggerTest, FixedSegmentsRejectPhysicalLengthDamage) {
  l_.reset();
  RemoveLogAndSegments(log_name_);
  constexpr size_t kSegment = 4096;
  for (const size_t length :
       {size_t{3}, kSegment, kSegment + 32, kSegment + 63, kSegment + 65}) {
    RemoveLogAndSegments(log_name_);
    {
      ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Logger>, fixed,
                            Logger::Create(log_name_, 32, 1, kSegment, true));
      ASSERT_SUCCESS(fixed->AddLog("acknowledged").GetStatus());
      ASSERT_SUCCESS(fixed->Finish());
    }
    std::filesystem::resize_file(log_name_, length);
    EXPECT_FALSE(Logger::Create(log_name_, 32, 1, kSegment).HasValue());
    ASSIGN_OR_ASSERT_FAIL(
        std::unique_ptr<Logger>, forced,
        Logger::Create(log_name_, 32, 60'000, kSegment, false, true));
    EXPECT_EQ(forced->BufferedLSN(), std::min(length, kSegment));
    ASSERT_SUCCESS(forced->TruncateTo(std::min(length, size_t{12})));
    ASSERT_SUCCESS(forced->Finish());
  }
}

TEST_F(LoggerTest, FixedSegmentsSyncFailureNeverAdvancesDurableEnd) {
  l_.reset();
  RemoveLogAndSegments(log_name_);
  ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Logger>, fixed,
                        Logger::Create(log_name_, 32, 60'000, 4096, true));
  fixed->SetCommitDrivenSync(true);
  ASSERT_SUCCESS(fixed->AddLog("first").GetStatus());
  ASSERT_SUCCESS(fixed->WaitForDurable(5));
  Logger::SetBeforeSyncHookForTest([](int) { return EIO; });
  ASSERT_SUCCESS(fixed->AddLog("unacknowledged").GetStatus());
  const Status status = fixed->WaitForDurable(fixed->BufferedLSN());
  Logger::SetBeforeSyncHookForTest({});
  EXPECT_NE(status, Status::kSuccess);
  EXPECT_EQ(fixed->DurableLSN(), 5);
  EXPECT_NE(fixed->Finish(), Status::kSuccess);
}

TEST_F(LoggerTest, FixedFrameWithoutMetadataIsNeverMigratedAsLegacyBytes) {
  l_.reset();
  RemoveLogAndSegments(log_name_);
  {
    ASSIGN_OR_ASSERT_FAIL(std::unique_ptr<Logger>, fixed,
                          Logger::Create(log_name_, 32, 1, 4096, true));
    ASSERT_SUCCESS(fixed->AddLog("committed").GetStatus());
    ASSERT_SUCCESS(fixed->Finish());
  }
  std::filesystem::remove(Logger::SegmentMetadataPath(log_name_));
  EXPECT_FALSE(Logger::Create(log_name_, 32, 1, 2048).HasValue());
  EXPECT_EQ(std::filesystem::file_size(log_name_), 4160);
  EXPECT_FALSE(std::filesystem::exists(Logger::SegmentPath(log_name_, 1)));
  EXPECT_FALSE(std::filesystem::exists(Logger::SegmentMetadataPath(log_name_)));
}
}  // namespace tinylamb
