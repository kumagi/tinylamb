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

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
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
    std::remove(log_name_.c_str());
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
    size_t random_size = (i * 31) % 40 + 1;
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
    std::string data(i * i + 1, 'x');
    lsn = l_->AddLog(data);
    EXPECT_EQ(lsn, size);
    size += i * i + 1;
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
}  // namespace tinylamb
