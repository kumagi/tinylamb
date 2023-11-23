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


#include "log_record.hpp"

#include <filesystem>

#include "gtest/gtest.h"

namespace tinylamb {

class LogRecordTest : public ::testing::Test {
 protected:
  static void SerializeDeserializeCheck(const LogRecord& log) {
    std::string serialized_log = log.Serialize();
    std::istringstream ss(serialized_log, std::istringstream::binary);
    LogRecord parsed_log;
    Decoder dec(ss);
    dec >> parsed_log;
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
    // InsertBranch related logs.
    SCOPED_TRACE("Insertion log tests");
    SerializeDeserializeCheck(
        LogRecord::InsertingLogRecord(12, 2, 1, 3, "hello"));
    SerializeDeserializeCheck(
        LogRecord::InsertingLeafLogRecord(12, 2, 3, "key", "hello"));
    SerializeDeserializeCheck(
        LogRecord::InsertingBranchLogRecord(12, 2, 3, "key", 343));

    // Compensation records.
    SerializeDeserializeCheck(
        LogRecord::CompensatingInsertLogRecord(12, 123, 345));
    SerializeDeserializeCheck(
        LogRecord::CompensatingInsertLogRecord(12, 34, "key1"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingInsertBranchLogRecord(12, 66, "key2"));
  }

  {
    // UpdateBranch related logs.
    SCOPED_TRACE("Updating log tests");
    SerializeDeserializeCheck(
        LogRecord::UpdatingLogRecord(13, 3, 3, 4, "redo_log", "long_undo_log"));
    SerializeDeserializeCheck(LogRecord::UpdatingLeafLogRecord(
        13, 3, 5, "key", "redo_log", "long_undo_log"));
    SerializeDeserializeCheck(
        LogRecord::UpdatingBranchLogRecord(13, 3, 5, "key", 123, 578));

    // Compensation records.
    SerializeDeserializeCheck(
        LogRecord::CompensatingUpdateLogRecord(12, 123, 345, "hello"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingUpdateLeafLogRecord(12, 854, "key2", "hello"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingUpdateBranchLogRecord(12, 854, "key4", 123));
  }

  {
    // Delete related logs.
    SCOPED_TRACE("Deletion log tests");
    SerializeDeserializeCheck(
        LogRecord::DeletingLogRecord(13, 4, 4, 5, "undo_log"));
    SerializeDeserializeCheck(
        LogRecord::DeletingLeafLogRecord(13, 4, 6, "key", "undo_log"));
    SerializeDeserializeCheck(
        LogRecord::DeletingBranchLogRecord(13, 4, 6, "key", 543));
    SerializeDeserializeCheck(
        LogRecord::CompensatingDeleteLogRecord(12, 123, 345, "deleted"));
    SerializeDeserializeCheck(LogRecord::CompensatingDeleteLeafLogRecord(
        12, 21343, "key3", "deleted"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingDeleteBranchLogRecord(12, 21343, "key3", 12312));
  }

  {
    // IndexKey related logs.
    SCOPED_TRACE("Fence log tests");
    SerializeDeserializeCheck(LogRecord::SetLowFenceLogRecord(
        20, 21, 12, IndexKey::MinusInfinity(), IndexKey("low")));
    SerializeDeserializeCheck(LogRecord::SetLowFenceLogRecord(
        2, 231, 112, IndexKey("previous"), IndexKey::MinusInfinity()));
    SerializeDeserializeCheck(LogRecord::SetLowFenceLogRecord(
        120, 1, 2, IndexKey("foobar"), IndexKey("low")));

    SerializeDeserializeCheck(LogRecord::SetHighFenceLogRecord(
        29, 51, 32, IndexKey::PlusInfinity(), IndexKey("high")));
    SerializeDeserializeCheck(LogRecord::SetHighFenceLogRecord(
        2, 5, 42, IndexKey("previous"), IndexKey::PlusInfinity()));
    SerializeDeserializeCheck(LogRecord::SetHighFenceLogRecord(
        21, 91, 12, IndexKey("foobar"), IndexKey("high")));
  }

  {
    // Foster Child related logs.
    SCOPED_TRACE("Foster log tests");
    SerializeDeserializeCheck(LogRecord::SetFosterLogRecord(
        20, 21, 12, FosterPair("new", 0), FosterPair("old", 43)));
    SerializeDeserializeCheck(LogRecord::SetFosterLogRecord(
        2, 1, 11, FosterPair("ne", 44), FosterPair("old", 1)));
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
  SerializeDeserializeCheck(
      LogRecord::SetLowestLogRecord(14, 123, 345, 687, 89));
}

}  // namespace tinylamb