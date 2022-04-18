//
// Created by kumagi on 2022/04/19.
//

#include "common/log_message.hpp"

#include <gtest/gtest.h>

TEST(LogMessage, Log) {
  LOG(FATAL) << "FATAL";
  LOG(ERROR) << "ERROR";
  LOG(ALERT) << "ALERT";
  LOG(WARN) << "WARN";
  LOG(NOTICE) << "NOTICE";
  LOG(INFO) << "INFO";
  LOG(USER) << "USER";
  LOG(DEBUG) << "DEBUG";
  LOG(TRACE) << "TRACE";
}