//
// Created by kumagi on 2021/06/01.
//

#include "logger.hpp"

#include <filesystem>

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
    l_ = std::make_unique<Logger>(log_name_, 4096, 10);
  }

  void TearDown() override {
    std::this_thread::sleep_for(std::chrono::microseconds(5));
    std::remove(log_name_.c_str());
  }

  void WaitForCommit(lsn_t target_lsn, size_t timeout_ms = 1000) {
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

TEST_F(LoggerTest, AppendBegin) {
  LogRecord l(0xcafebabe, 0xdeadbeef, LogType::kBegin);
  lsn_t lsn = l_->AddLog(l);
  ASSERT_EQ(0, lsn);  // Inserted place must be the beginning of the log.
  WaitForCommit(0 + l.Size());
  EXPECT_EQ(std::filesystem::file_size(log_name_), l.Size());
}

TEST_F(LoggerTest, AppendMany) {
  lsn_t lsn = 0;
  size_t size = 0;
  for (int i = 0; i < 2048; ++i) {
    LogRecord l = LogRecord::InsertingLogRecord(
        0, 0, i, i * 2, RandomString((i * 31) % 800 + 1));
    lsn = l_->AddLog(l) + l.Size();
    size += l.Size();
  }
  WaitForCommit(lsn);
  EXPECT_EQ(std::filesystem::file_size(log_name_), size);
}

}  // namespace tinylamb