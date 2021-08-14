//
// Created by kumagi on 2021/06/01.
//

#include "log_record.hpp"

#include <filesystem>

#include "gtest/gtest.h"

namespace tinylamb {

class LogRecordTest : public ::testing::Test {
 protected:
  static void SerializeDeserializeCheck(const LogRecord& log) {
    std::string serialized_log = log.Serialize();
    LogRecord parsed_log;
    LogRecord::ParseLogRecord(serialized_log, &parsed_log);
    EXPECT_EQ(log, parsed_log);
  }
};

TEST_F(LogRecordTest, construct) {
  LogRecord l(0xaabbccddeeff0011, 0x1122334455667788, LogType::kBegin);
}

TEST_F(LogRecordTest, check) {
  LogRecord begin_log(0xaabbccddeeff0011, 0x1122334455667788, LogType::kBegin);
  SerializeDeserializeCheck(begin_log);

  LogRecord insert_log =
      LogRecord::InsertingLogRecord(12, 2, RowPosition(1, 2), "hello");
  SerializeDeserializeCheck(insert_log);

  LogRecord update_log = LogRecord::UpdatingLogRecord(
      13, 3, RowPosition(3, 4), "redo_log", "long_undo_log");
  SerializeDeserializeCheck(update_log);

  LogRecord delete_log =
      LogRecord::DeletingLogRecord(13, 4, RowPosition(4, 5), "undo_log");
  SerializeDeserializeCheck(delete_log);

  LogRecord checkpoint_log =
      LogRecord::CheckpointLogRecord(14, 5, {1, 2, 3}, {4, 5, 6});
  SerializeDeserializeCheck(checkpoint_log);

  LogRecord allocate_log =
      LogRecord::AllocatePageLogRecord(15, 7, 10, PageType::kMetaPage);
  SerializeDeserializeCheck(allocate_log);

  LogRecord destroy_log = LogRecord::DestroyPageLogRecord(16, 8, 21);
  SerializeDeserializeCheck(destroy_log);
}

}  // namespace tinylamb