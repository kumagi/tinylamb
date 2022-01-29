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
    LogRecord::ParseLogRecord(serialized_log.data(), &parsed_log);
    EXPECT_EQ(log, parsed_log);
  }
};

TEST_F(LogRecordTest, construct) {
  LogRecord l(0xaabbccddeeff0011, 0x1122334455667788, LogType::kBegin);
}

TEST_F(LogRecordTest, check) {
  SerializeDeserializeCheck(
      LogRecord(0xaabbccddeeff0011, 0x1122334455667788, LogType::kBegin));

  {
    // Insert related logs.
    SCOPED_TRACE("Insertion log tests");
    SerializeDeserializeCheck(
        LogRecord::InsertingLogRecord(12, 2, 1, 3, "hello"));
    SerializeDeserializeCheck(
        LogRecord::InsertingLogRecord(12, 2, 3, "key", "hello"));
    SerializeDeserializeCheck(
        LogRecord::InsertingLogRecord(12, 2, 3, "key", 343));

    // Compensation records.
    SerializeDeserializeCheck(
        LogRecord::CompensatingInsertLogRecord(12, 123, 345));
    SerializeDeserializeCheck(
        LogRecord::CompensatingInsertLogRecord(12, 34, "key1"));
  }

  {
    // Update related logs.
    SCOPED_TRACE("Updating log tests");
    SerializeDeserializeCheck(
        LogRecord::UpdatingLogRecord(13, 3, 3, 4, "redo_log", "long_undo_log"));
    SerializeDeserializeCheck(LogRecord::UpdatingLogRecord(
        13, 3, 5, "key", "redo_log", "long_undo_log"));
    SerializeDeserializeCheck(
        LogRecord::UpdatingLogRecord(13, 3, 5, "key", 123, 578));

    // Compensation records.
    SerializeDeserializeCheck(
        LogRecord::CompensatingUpdateLogRecord(12, 123, 345, "hello"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingUpdateLogRecord(12, 854, "key2", "hello"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingUpdateLogRecord(12, 854, "key4", 123));
  }

  {
    // Delete related logs.
    SCOPED_TRACE("Deletion log tests");
    SerializeDeserializeCheck(
        LogRecord::DeletingLogRecord(13, 4, 4, 5, "undo_log"));
    SerializeDeserializeCheck(
        LogRecord::DeletingLogRecord(13, 4, 6, "key", "undo_log"));
    SerializeDeserializeCheck(
        LogRecord::DeletingLogRecord(13, 4, 6, "key", 543));
    SerializeDeserializeCheck(
        LogRecord::CompensatingDeleteLogRecord(12, 123, 345, "deleted"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingDeleteLogRecord(12, 21343, "key3", "deleted"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingDeleteLogRecord(12, 21343, "key3", 12312));
  }

  // Checkpoint related logs.
  SerializeDeserializeCheck(LogRecord::BeginCheckpointLogRecord());
  SerializeDeserializeCheck(LogRecord::EndCheckpointLogRecord(
      {{1, 2}, {3, 4}, {5, 6}}, {{4LLU, TransactionStatus::kRunning, 5LLU},
                                 {5, TransactionStatus::kCommitted, 6},
                                 {6, TransactionStatus::kAborted, 7}}));

  // Page manipulation related logs.
  SerializeDeserializeCheck(
      LogRecord::AllocatePageLogRecord(15, 7, 10, PageType::kMetaPage));
  SerializeDeserializeCheck(LogRecord::DestroyPageLogRecord(16, 8, 21));

  // Lowest value log.
  SerializeDeserializeCheck(LogRecord::SetLowestLogRecord(14, 123, 345, 687));
}

}  // namespace tinylamb