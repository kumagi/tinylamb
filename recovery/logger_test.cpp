//
// Created by kumagi on 2021/06/01.
//

#include "logger.hpp"

#include <filesystem>

#include "gtest/gtest.h"
#include "recovery/log_record.hpp"
#include "type/row.hpp"

namespace tinylamb {

class LoggerTest : public ::testing::Test {
 protected:
  static constexpr char kFileName[] = "logger_test.log";
  void SetUp() override { l_ = std::make_unique<Logger>(kFileName, 4096, 10); }

  void TearDown() override {
    std::this_thread::sleep_for(std::chrono::microseconds(5));
    std::remove(kFileName);
  }

  void WaitForCommit(lsn_t target_lsn, size_t timeout_ms = 1000) {
    size_t counter = 0;
    while (l_->CommittedLSN() != target_lsn && counter < timeout_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      ++counter;
    }
    EXPECT_LT(counter, timeout_ms);
  }

  std::unique_ptr<Logger> l_;
};

TEST_F(LoggerTest, Construct) {
  // Do nothing.
}

TEST_F(LoggerTest, AppendBegin) {
  LogRecord l(0xcafebabe, 0xdeadbeef, LogType::kBegin);
  lsn_t lsn = l_->AddLog(l);
  ASSERT_EQ(0, lsn);  // Inserted place must be the beginning of the log.
  WaitForCommit(lsn + l.Size());
  EXPECT_EQ(std::filesystem::file_size(kFileName), l.Size());
}

TEST_F(LoggerTest, AppendManyBegin) {
  LogRecord l(0, 0, LogType::kBegin);
  lsn_t lsn = 0;
  for (int i = 0; i < 100; ++i) {
    lsn = l_->AddLog(l);
  }
  WaitForCommit(lsn + l.Size());
  EXPECT_EQ(std::filesystem::file_size(kFileName), l.Size() * 100);
}

}  // namespace tinylamb