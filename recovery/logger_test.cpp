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

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "gtest/gtest.h"
#include "recovery/log_record.hpp"

namespace tinylamb {
class LoggerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string prefix = "logger_test-" + RandomString();
    log_name_ = prefix + ".log";
    l_ = std::make_unique<Logger>(log_name_, 32, 1);
  }

  void TearDown() override {
    std::this_thread::sleep_for(std::chrono::microseconds(5));
    // Best-effort cleanup; a missing file must not fail the test.
    (void)std::remove(log_name_.c_str());
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

TEST_F(LoggerTest, Construct) {
  // Arrange -- nothing to set up; default Logger constructed by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(LoggerTest, AppendOne) {
  // Arrange -- one LogRecord (kBegin) and a fresh Logger with 32 KiB buffer
  LogRecord l(0xcafebabe, 0xdeadbeef, LogType::kBegin);

  // Act -- append the serialized log; wait for commit; read back via ifstream
  lsn_t lsn = l_->AddLog(l.Serialize());
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

TEST_F(LoggerTest, AppendTwo) {
  // Arrange -- two deterministic strings d1/d2 of known lengths
  std::string d1("6uRa9BIQb5RD2p8dIxXKtpgIDU1HBT7wfqfdZDApAqX5crm36WaCgRXgQ");
  std::string d2("P16dKMXY5TvrZVU7bKqLuAdf636mxmSsZpaDkocoClSZs3pX3");

  // Act -- append d1, sleep 1us, append d2; wait for commit; read back via ifstream
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

  // Act -- append each random string; accumulate LSN and total size; wait for commit; read back file size
  for (int i = 0; i < 64; ++i) {
    size_t random_size = ((i * 31) % 40) + 1;
    lsn = l_->AddLog(RandomString(random_size)) + random_size;
    size += random_size;
    EXPECT_EQ(lsn, size);
  }
  WaitForCommit(lsn);

  // Assert -- committed LSN equals total size and file size matches
  EXPECT_EQ(std::filesystem::file_size(log_name_), size);
}

TEST_F(LoggerTest, AppendExponential) {
  // Arrange -- reset Logger to a fresh instance; prepare 1000 exponential-size strings ('x' repeated)
  l_ = std::make_unique<Logger>(log_name_);
  lsn_t lsn = 0;
  size_t size = 0;

  // Act -- append each string; accumulate LSN and total size; wait for commit; read back file size
  for (int i = 0; i < 1000; ++i) {
    std::string data((i * i) + 1, 'x');
    lsn = l_->AddLog(data);
    EXPECT_EQ(lsn, size);
    size += (i * i) + 1;
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
  const lsn_t lsn = l_->AddLog(payload);
  l_->WaitForDurable(lsn + payload.size());
  EXPECT_GE(l_->DurableLSN(), lsn + payload.size());
  EXPECT_GE(l_->CommittedLSN(), lsn + payload.size());
}

TEST_F(LoggerTest, FinishDrainsWithoutSpin) {
  std::string payload(4096, 'z');
  l_->AddLog(payload);
  l_->Finish();
  EXPECT_EQ(l_->CommittedLSN(), payload.size());
  EXPECT_GE(l_->DurableLSN(), payload.size());
  l_.reset();
}

TEST_F(LoggerTest, ReopenExistingWalKeepsContent) {
  // Regression: the worker used to start before flushed_lsn_/buffered_lsn_
  // were seeded with the on-disk size, so reopening an existing WAL could
  // append zero-filled buffer bytes over the tail of the log.
  const std::string first(128, 'x');
  ASSERT_EQ(l_->AddLog(first), 0);
  l_->WaitForDurable(first.size());
  l_.reset();

  auto second = std::make_unique<Logger>(log_name_, 32, 1);
  EXPECT_EQ(second->BufferedLSN(), first.size());
  const std::string appended(64, 'y');
  const lsn_t lsn = second->AddLog(appended);
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
  // Arrange -- point the log fd at /dev/full so every write() fails with
  // ENOSPC while the fd itself stays valid.
  const int full_fd = ::open("/dev/full", O_WRONLY);
  ASSERT_GE(full_fd, 0);
  ASSERT_EQ(::dup2(full_fd, l_->Fd()), l_->Fd());
  ASSERT_EQ(::close(full_fd), 0);

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
  EXPECT_EQ(l_->ErrorNumber(), ENOSPC);
  EXPECT_THROW(l_->AddLog(std::string(16, 'b')), std::runtime_error);
  EXPECT_THROW(l_->WaitForDurable(l_->BufferedLSN() + 4096),
               std::runtime_error);
  EXPECT_THROW(l_->Finish(), std::runtime_error);
}

TEST_F(LoggerTest, AddLogReturnsFreshLsnAfterBufferFullWait) {
  // Regression: a producer parked in the full-buffer wait used to return its
  // stale pre-wait LSN even though another producer had consumed that LSN
  // space, corrupting prev_lsn chains.
  constexpr size_t kBufSize = 512;
  l_.reset();
  auto logger = std::make_unique<Logger>(log_name_, kBufSize, 1);

  // Arrange -- exactly fill the ring buffer so both producers must park.
  logger->AddLog(std::string(kBufSize, 'f'));
  lsn_t first_lsn = 0;
  lsn_t second_lsn = 0;

  // Act -- two concurrent producers race for the freed space.
  std::thread a([&] { first_lsn = logger->AddLog(std::string(16, 'a')); });
  std::thread b([&] { second_lsn = logger->AddLog(std::string(16, 'b')); });
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
}  // namespace tinylamb
