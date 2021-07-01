//
// Created by kumagi on 2021/06/01.
//

#include "log_record.hpp"

#include <filesystem>

#include "gtest/gtest.h"

namespace tinylamb {

class LogRecordTest : public ::testing::Test {
 protected:
};

TEST_F(LogRecordTest, construct) {
  LogRecord l(0xaabbccddeeff0011, 0x1122334455667788, LogType::kBegin);
}

}  // namespace tinylamb