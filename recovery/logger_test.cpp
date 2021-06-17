//
// Created by kumagi on 2021/06/01.
//

#include <filesystem>

#include "gtest/gtest.h"
#include "logger.hpp"

namespace tinylamb {

class LoggerTest : public ::testing::Test {
protected:
  static constexpr char kFileName[] = "logger_test.log";
  void SetUp() override {
    l_ = std::make_unique<Logger>(kFileName, 1024, 1);
  }

  void TearDown() override {
    std::this_thread::sleep_for(std::chrono::microseconds(5));
    std::remove(kFileName);
  }

  void WaitForCommit(uint64_t target_lsn, size_t timeout_ms = 100) {
    size_t counter = 0;
    while (l_->CommittedLSN() != target_lsn && counter < timeout_ms) {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
      ++counter;
    }
    EXPECT_LT(counter, timeout_ms);
  }

  std::unique_ptr<Logger> l_;
};

TEST_F(LoggerTest, Construct) {
  // Do nothing.
}

TEST_F(LoggerTest, AppendOne) {
  std::string data(1000, 'd');
  EXPECT_TRUE(l_->AddLog(8, data));
  WaitForCommit(8);
  EXPECT_EQ(std::filesystem::file_size(kFileName), 1000);
}

TEST_F(LoggerTest, AppendWithLength) {
  std::string data(1000, 'd');
  EXPECT_TRUE(l_->AddLog(7, data.c_str(), 500));
  WaitForCommit(7);
  EXPECT_EQ(std::filesystem::file_size(kFileName), 500);
}

TEST_F(LoggerTest, AppendTooBig) {
  std::string data(1024, 'd');
  EXPECT_FALSE(l_->AddLog(9, data));
  EXPECT_EQ(std::filesystem::file_size(kFileName), 0);
}

TEST_F(LoggerTest, AppendMany) {
  int counter = 1;
  while (counter <= 10) {
    std::string data(1000, 'a' + counter);
    if (l_->AddLog(counter, data)) {
      ++counter;
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  }
  WaitForCommit(10);
  EXPECT_EQ(std::filesystem::file_size(kFileName), 10 * 1000);
}

}  // namespace tinylamb