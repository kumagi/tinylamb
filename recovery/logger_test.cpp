//
// Created by kumagi on 2021/06/01.
//

#include "logger.hpp"

#include <filesystem>
#include <fstream>

#include "common/random_string.hpp"
#include "gtest/gtest.h"
#include "recovery/log_record.hpp"
#include "type/row.hpp"

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
  // Do nothing.
}

TEST_F(LoggerTest, AppendOne) {
  LogRecord l(0xcafebabe, 0xdeadbeef, LogType::kBegin);
  lsn_t lsn = l_->AddLog(l.Serialize());
  ASSERT_EQ(0, lsn);  // Inserted place must be the beginning of the log.
  WaitForCommit(0 + l.Size());
  EXPECT_EQ(std::filesystem::file_size(log_name_), l.Size());
}

TEST_F(LoggerTest, AppendTwo) {
  std::string d1("6uRa9BIQb5RD2p8dIxXKtpgIDU1HBT7wfqfdZDApAqX5crm36WaCgRXgQ");
  std::string d2("P16dKMXY5TvrZVU7bKqLuAdf636mxmSsZpaDkocoClSZs3pX3");
  l_->AddLog(d1);
  std::this_thread::sleep_for(std::chrono::microseconds(1));
  l_->AddLog(d2);
  WaitForCommit(d1.size() + d2.size());
  EXPECT_EQ(std::filesystem::file_size(log_name_), d1.size() + d2.size());
  std::ifstream file;
  file.open(log_name_);
  std::string file_data;
  file >> file_data;
  ASSERT_EQ(file_data, d1 + d2);
}

TEST_F(LoggerTest, AppendMany) {
  lsn_t lsn = 0;
  size_t size = 0;
  for (int i = 0; i < 64; ++i) {
    size_t random_size = (i * 31) % 40 + 1;
    lsn = l_->AddLog(RandomString(random_size)) + random_size;
    size += random_size;
    EXPECT_EQ(lsn, size);
  }
  WaitForCommit(lsn);
  EXPECT_EQ(std::filesystem::file_size(log_name_), size);
}

TEST_F(LoggerTest, Verify) {
  std::string written_log = RandomString(1024);
  l_->AddLog(written_log);
  WaitForCommit(written_log.size());
  std::ifstream file;
  file.open(log_name_);
  std::string file_data;
  file >> file_data;
  ASSERT_EQ(file_data, written_log);
}

}  // namespace tinylamb