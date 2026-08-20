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

#include <sstream>
#include <string>

#include "common/decoder.hpp"
#include "gtest/gtest.h"
#include "page/index_key.hpp"
#include "page/page_type.hpp"
#include "transaction/transaction.hpp"

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
  // Arrange -- nothing more than a LogRecord with deterministic txn/offset/type
  // Act -- default-construct a LogRecord via its 3-arg ctor
  LogRecord l(0xaabbccddeeff0011, 0x1122334455667788, LogType::kBegin);
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(LogRecordTest, check) {
  // Arrange -- a generic LogRecord for round-trip serdes, plus per-category sub-arranges under SCOPED_TRACE
  SerializeDeserializeCheck(
      LogRecord(0xaabbccddeeff0011, 0x1122334455667788, LogType::kBegin));

  {
    // Arrange -- InsertBranch-related logs (Inserting/InsertingLeaf/InsertingBranch + compensations)
    SCOPED_TRACE("Insertion log tests");
    // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality for each log variant
    SerializeDeserializeCheck(
        LogRecord::InsertingLogRecord(12, 2, 1, 3, "hello"));
    SerializeDeserializeCheck(
        LogRecord::InsertingLeafLogRecord(12, 2, 3, "key", "hello"));
    SerializeDeserializeCheck(
        LogRecord::InsertingBranchLogRecord(12, 2, 3, "key", 343));

    SerializeDeserializeCheck(
        LogRecord::CompensatingInsertLogRecord(12, 123, 345));
    SerializeDeserializeCheck(
        LogRecord::CompensatingInsertLogRecord(12, 34, "key1"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingInsertBranchLogRecord(12, 66, "key2"));
  }

  {
    // Arrange -- UpdateBranch-related logs (Updating/UpdatingLeaf/UpdatingBranch + compensations)
    SCOPED_TRACE("Updating log tests");
    // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality for each log variant
    SerializeDeserializeCheck(
        LogRecord::UpdatingLogRecord(13, 3, 3, 4, "redo_log", "long_undo_log"));
    SerializeDeserializeCheck(LogRecord::UpdatingLeafLogRecord(
        13, 3, 5, "key", "redo_log", "long_undo_log"));
    SerializeDeserializeCheck(
        LogRecord::UpdatingBranchLogRecord(13, 3, 5, "key", 123, 578));

    SerializeDeserializeCheck(
        LogRecord::CompensatingUpdateLogRecord(12, 123, 345, "hello"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingUpdateLeafLogRecord(12, 854, "key2", "hello"));
    SerializeDeserializeCheck(
        LogRecord::CompensatingUpdateBranchLogRecord(12, 854, "key4", 123));
  }

  {
    // Arrange -- Delete-related logs (Deleting/DeletingLeaf/DeletingBranch + compensations)
    SCOPED_TRACE("Deletion log tests");
    // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality for each log variant
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
    // Arrange -- IndexKey-related logs (SetLowFence / SetHighFence with various key boundaries)
    SCOPED_TRACE("Fence log tests");
    // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality for each fence log
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
    // Arrange -- Foster Child-related logs (SetFoster with FosterPair entries)
    SCOPED_TRACE("Foster log tests");
    // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality for each foster log
    SerializeDeserializeCheck(LogRecord::SetFosterLogRecord(
        20, 21, 12, FosterPair("new", 0), FosterPair("old", 43)));
    SerializeDeserializeCheck(LogRecord::SetFosterLogRecord(
        2, 1, 11, FosterPair("ne", 44), FosterPair("old", 1)));
  }

  // Arrange -- Checkpoint-related logs (BeginCheckpoint / EndCheckpoint with txn table)
  // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality
  SerializeDeserializeCheck(LogRecord::BeginCheckpointLogRecord());
  SerializeDeserializeCheck(LogRecord::EndCheckpointLogRecord(
      {{1, 2}, {3, 4}, {5, 6}}, {{4LLU, TransactionStatus::kRunning, 5LLU},
                                 {5, TransactionStatus::kCommitted, 6},
                                 {6, TransactionStatus::kAborted, 7}}));

  // Arrange -- Page manipulation logs (AllocatePage / DestroyPage)
  // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality
  SerializeDeserializeCheck(
      LogRecord::AllocatePageLogRecord(15, 7, 10, PageType::kMetaPage));
  SerializeDeserializeCheck(LogRecord::DestroyPageLogRecord(16, 8, 21));

  // Arrange -- Lowest value log (SetLowest with 5 fields)
  // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality
  SerializeDeserializeCheck(
      LogRecord::SetLowestLogRecord(14, 123, 345, 687, 89));
}

TEST_F(LogRecordTest, SerializationSizeMatchesSerialize) {
  // Arrange -- one record of every serializable category
  std::vector<LogRecord> records{
      LogRecord(0xdeadbeef, 77, LogType::kBegin),
      LogRecord(0xdeadbeef, 77, LogType::kCommit),
      LogRecord::InsertingLogRecord(1, 2, 3, 4, "redo"),
      LogRecord::InsertingLeafLogRecord(1, 2, 3, "key", "redo"),
      LogRecord::InsertingBranchLogRecord(1, 2, 3, "key", 44),
      LogRecord::UpdatingLogRecord(1, 2, 3, 4, "redo", "undo"),
      LogRecord::UpdatingLeafLogRecord(1, 2, 3, "key", "redo", "undo"),
      LogRecord::UpdatingBranchLogRecord(1, 2, 3, "key", 5, 6),
      LogRecord::DeletingLogRecord(1, 2, 3, 4, "undo"),
      LogRecord::DeletingLeafLogRecord(1, 2, 3, "key", "undo"),
      LogRecord::DeletingBranchLogRecord(1, 2, 3, "key", 5),
      LogRecord::CompensatingInsertLogRecord(2, 3, 4),
      LogRecord::CompensatingInsertLogRecord(2, 3, "key"),
      LogRecord::CompensatingInsertBranchLogRecord(2, 3, "key"),
      LogRecord::CompensatingUpdateLogRecord(2, 3, 4, "redo"),
      LogRecord::CompensatingUpdateLeafLogRecord(2, 3, "key", "redo"),
      LogRecord::CompensatingUpdateBranchLogRecord(2, 3, "key", 5),
      LogRecord::CompensatingDeleteLogRecord(2, 3, 4, "redo"),
      LogRecord::CompensatingDeleteLeafLogRecord(2, 3, "key", "redo"),
      LogRecord::CompensatingDeleteBranchLogRecord(2, 3, "key", 5),
      LogRecord::SetLowFenceLogRecord(1, 2, 3, IndexKey("redo"),
                                     IndexKey("undo")),
      LogRecord::SetHighFenceLogRecord(1, 2, 3, IndexKey("redo"),
                                      IndexKey("undo")),
      LogRecord::SetFosterLogRecord(1, 2, 3, FosterPair("new", 4),
                                    FosterPair("old", 5)),
      LogRecord::SetLowestLogRecord(1, 2, 3, 4, 5),
      LogRecord::AllocatePageLogRecord(1, 2, 3, PageType::kRowPage),
      LogRecord::DestroyPageLogRecord(1, 2, 3),
      LogRecord::BeginCheckpointLogRecord(),
      LogRecord::EndCheckpointLogRecord(
          {{1, 2}}, {{3, TransactionStatus::kCommitted, 4}}),
  };

  // Act + Assert -- the serialized byte count always matches Size()
  for (const auto& log : records) {
    SCOPED_TRACE(static_cast<uint16_t>(log.type));
    ASSERT_EQ(log.Serialize().size(), log.Size());
  }
}

TEST_F(LogRecordTest, HasSlotAndPageIdFlags) {
  // Arrange -- records with and without a page id / slot / key
  LogRecord begin = LogRecord(1, 2, LogType::kBegin);
  LogRecord row = LogRecord::InsertingLogRecord(1, 2, 3, 4, "redo");
  LogRecord leaf = LogRecord::InsertingLeafLogRecord(1, 2, 3, "key", "redo");

  // Act -- query the flag helpers
  // Assert -- begin carries neither field; row carries both; leaf has pid but
  // no slot
  EXPECT_FALSE(begin.HasPageID());
  EXPECT_FALSE(begin.HasSlot());
  EXPECT_TRUE(row.HasPageID());
  EXPECT_TRUE(row.HasSlot());
  EXPECT_TRUE(leaf.HasPageID());
  EXPECT_FALSE(leaf.HasSlot());
}

TEST_F(LogRecordTest, LogTypeStreamOperator) {
  // Arrange -- every defined LogType value
  const std::vector<LogType> types{
      LogType::kUnknown,      LogType::kBegin,
      LogType::kInsertRow,    LogType::kInsertLeaf,
      LogType::kInsertBranch, LogType::kUpdateRow,
      LogType::kUpdateLeaf,   LogType::kUpdateBranch,
      LogType::kDeleteRow,    LogType::kDeleteLeaf,
      LogType::kDeleteBranch, LogType::kSetLowFence,
      LogType::kSetHighFence, LogType::kSetFoster,
      LogType::kCompensateInsertRow,    LogType::kCompensateInsertLeaf,
      LogType::kCompensateInsertBranch, LogType::kCompensateUpdateRow,
      LogType::kCompensateUpdateLeaf,   LogType::kCompensateUpdateBranch,
      LogType::kCompensateDeleteRow,    LogType::kCompensateDeleteLeaf,
      LogType::kCompensateDeleteBranch, LogType::kCompensateSetLowFence,
      LogType::kCompensateSetHighFence, LogType::kCompensateSetFoster,
      LogType::kCommit,       LogType::kBeginCheckpoint,
      LogType::kEndCheckpoint, LogType::kSystemAllocPage,
      LogType::kSystemDestroyPage, LogType::kLowestValue};

  // Act -- stream each type
  // Assert -- the common types print their canonical names
  for (const LogType type : types) {
    std::stringstream ss;
    ss << type;
    EXPECT_FALSE(ss.str().empty()) << static_cast<uint16_t>(type);
  }
  std::stringstream begin;
  begin << LogType::kBegin;
  EXPECT_EQ(begin.str(), "BEGIN");
  std::stringstream unknown;
  unknown << static_cast<LogType>(0xffff);
  EXPECT_NE(unknown.str().find("undefined"), std::string::npos);
}

TEST_F(LogRecordTest, DumpOperatorCoversAllRecordCategories) {
  // Arrange -- one representative record per operator<< switch arm
  std::vector<LogRecord> records{
      LogRecord::InsertingLogRecord(12, 3, 1, 2, "redo"),
      LogRecord::InsertingLeafLogRecord(12, 3, 1, "key", "redo"),
      LogRecord::InsertingBranchLogRecord(12, 3, 1, "key", 33),
      LogRecord::UpdatingLogRecord(12, 3, 1, 2, "redo", "undo"),
      LogRecord::UpdatingLeafLogRecord(12, 3, 1, "key", "redo", "undo"),
      LogRecord::UpdatingBranchLogRecord(12, 3, 1, "key", 4, 5),
      LogRecord::DeletingLogRecord(12, 3, 1, 2, "undo"),
      LogRecord::DeletingLeafLogRecord(12, 3, 1, "key", "undo"),
      LogRecord::DeletingBranchLogRecord(12, 3, 1, "key", 4),
      LogRecord::CompensatingInsertLogRecord(3, 1, 2),
      LogRecord::CompensatingInsertBranchLogRecord(3, 1, "key"),
      LogRecord::CompensatingUpdateLeafLogRecord(3, 1, "key", "redo"),
      LogRecord::CompensatingUpdateBranchLogRecord(3, 1, "key", 4),
      LogRecord::CompensatingDeleteLeafLogRecord(3, 1, "key", "redo"),
      LogRecord::CompensatingDeleteBranchLogRecord(3, 1, "key", 4),
      LogRecord::SetFosterLogRecord(12, 3, 1, FosterPair("new", 4),
                                    FosterPair("old", 5)),
      LogRecord::SetLowestLogRecord(12, 3, 1, 4, 5),
      LogRecord::CompensateSetLowFenceLogRecord(12, 3, 1, IndexKey("redo")),
      LogRecord::CompensateSetHighFenceLogRecord(12, 3, 1, IndexKey("redo")),
      LogRecord::CompensateSetFosterLogRecord(12, 3, 1, FosterPair("new", 4)),
      LogRecord::AllocatePageLogRecord(12, 3, 1, PageType::kLeafPage),
      LogRecord::DestroyPageLogRecord(12, 3, 1),
      LogRecord::BeginCheckpointLogRecord(),
      LogRecord::EndCheckpointLogRecord(
          {{1, 2}, {3, 4}}, {{4, TransactionStatus::kCommitted, 5}}),
      LogRecord(12, 3, LogType::kBegin),
      LogRecord(12, 3, LogType::kCommit),
  };

  // Act -- stream every record
  // Assert -- each dump is non-empty and mentions its transaction id (the two
  // checkpoint records return early and only print their type tag)
  for (const auto& log : records) {
    std::stringstream ss;
    ss << log;
    EXPECT_FALSE(ss.str().empty());
    if (log.type == LogType::kBeginCheckpoint ||
        log.type == LogType::kEndCheckpoint) {
      continue;
    }
    EXPECT_NE(ss.str().find("txn_id: 3"), std::string::npos)
        << static_cast<uint16_t>(log.type);
  }

  // Assert -- checkpoint records expose their tables
  std::stringstream end_checkpoint;
  end_checkpoint << LogRecord::EndCheckpointLogRecord(
      {{1, 2}}, {{3, TransactionStatus::kRunning, 4}});
  EXPECT_NE(end_checkpoint.str().find("DPT"), std::string::npos);
  EXPECT_NE(end_checkpoint.str().find("TT"), std::string::npos);
}

TEST_F(LogRecordTest, DumpPositionOmittedString) {
  // Arrange -- a leaf insert with a key longer than the 20-byte dump threshold
  LogRecord long_key =
      LogRecord::InsertingLeafLogRecord(12, 3, 1, std::string(100, 'x'),
                                        "redo");
  std::stringstream ss;
  ss << long_key;
  // Act + Assert -- the truncated form keeps a head + tail and marks the
  // elided bytes
  EXPECT_NE(ss.str().find("..("), std::string::npos);
  EXPECT_NE(ss.str().find("bytes).."), std::string::npos);

  // Act + Assert -- short keys are printed verbatim
  LogRecord short_key =
      LogRecord::InsertingLeafLogRecord(12, 3, 1, "tiny", "redo");
  std::stringstream short_ss;
  short_ss << short_key;
  EXPECT_NE(short_ss.str().find("tiny"), std::string::npos);
}

TEST_F(LogRecordTest, CompensatingFenceFosterAndLowestConstructors) {
  // Arrange -- construct the record types that were never exercised before
  LogRecord low_fence =
      LogRecord::CompensateSetLowFenceLogRecord(12, 3, 1, IndexKey("redo"));
  LogRecord high_fence =
      LogRecord::CompensateSetHighFenceLogRecord(12, 3, 1, IndexKey("redo"));
  LogRecord foster =
      LogRecord::CompensateSetFosterLogRecord(12, 3, 1, FosterPair("new", 4));
  LogRecord lowest =
      LogRecord::CompensateSetLowestValueLogRecord(3, 1, 4);

  // Act + Assert -- the compensating log types and fields are set correctly
  EXPECT_EQ(low_fence.type, LogType::kCompensateSetLowFence);
  EXPECT_EQ(high_fence.type, LogType::kCompensateSetHighFence);
  EXPECT_EQ(foster.type, LogType::kCompensateSetFoster);
  EXPECT_EQ(lowest.type, LogType::kLowestValue);
  EXPECT_EQ(low_fence.txn_id, 3);
  EXPECT_EQ(low_fence.pid, 1);
  EXPECT_EQ(low_fence.Serialize().size(), low_fence.Size());
  EXPECT_EQ(high_fence.Serialize().size(), high_fence.Size());
  EXPECT_EQ(foster.Serialize().size(), foster.Size());
  EXPECT_EQ(lowest.Serialize().size(), lowest.Size());
  EXPECT_EQ(lowest.redo_page, 4);

  // Act + Assert -- foster and lowest round-trip through the decoder; the two
  // compensating fence types are intentionally not decoded because the decoder
  // has no case for kCompensateSetLowFence / kCompensateSetHighFence and
  // aborts in the default arm.
  SerializeDeserializeCheck(foster);
  SerializeDeserializeCheck(lowest);
}

TEST_F(LogRecordTest, DecodeMissingCasesForCompensatingFences) {
  // Known gap: Decoder& operator>> has no case for kCompensateSetLowFence or
  // kCompensateSetHighFence (it falls through to `default: assert(!"unknown
  // log")`).  Encoding works, so the serialized form is stable; only the
  // decode direction is missing.  This test pins the encoder side and
  // documents that the round-trip must remain uncovered until the decoder
  // gains the two cases.
  LogRecord low_fence =
      LogRecord::CompensateSetLowFenceLogRecord(12, 3, 1, IndexKey("redo"));
  LogRecord high_fence =
      LogRecord::CompensateSetHighFenceLogRecord(12, 3, 1, IndexKey("redo"));
  EXPECT_EQ(low_fence.Serialize().size(), low_fence.Size());
  EXPECT_EQ(high_fence.Serialize().size(), high_fence.Size());
}

TEST_F(LogRecordTest, DecodeHugeEndCheckpointTableSizeRejected) {
  // Fuzzer regression (log_record_fuzzer): a kEndCheckpoint record whose
  // encoded dirty_page_table / active_transaction_table size field is huge
  // makes Decoder::operator>>(std::vector<T>&) (common/decoder.hpp:43) call
  // vec.resize(size) with no bound against the remaining input bytes
  // (recovery/log_record.cpp:836-838).  A malicious or corrupt WAL record then
  // triggers an unbounded allocation (OOM) or a std::length_error thrown from
  // resize, which propagates out of the decoder and std::terminate()s the
  // process.  Decoding must reject the record cleanly instead of allocating an
  // unbounded vector.  This test currently FAILS: the decode throws
  // std::length_error and the EXPECT_NO_THROW / empty-table assertions fail.
  const auto kEndCheckpointPrefix = [] {
    std::string bytes;
    bytes.append(1, 0x1c).append(1, 0x00);  // LogType::kEndCheckpoint (uint16)
    bytes.append(8, '\x00');                // prev_lsn
    bytes.append(8, '\x00');                // txn_id
    bytes.append(1, '\x00');                // types: no pid / slot / key
    return bytes;
  }();
  const std::string huge_size(8, '\x20');  // 0x2020202020202020 elements

  // Arrange -- a kEndCheckpoint whose dirty_page_table claims a huge length.
  {
    std::istringstream ss(kEndCheckpointPrefix + huge_size,
                          std::istringstream::binary);
    LogRecord record;
    Decoder dec(ss);

    // Act + Assert -- decoding must not allocate an unbounded vector; it
    // should reject the record cleanly.
    EXPECT_NO_THROW(dec >> record);  // FAILS today: std::length_error
    EXPECT_TRUE(record.dirty_page_table.empty());
  }

  // Arrange -- the same attack on active_transaction_table.
  {
    std::string bytes = kEndCheckpointPrefix;
    bytes.append(8, '\x00');  // empty dirty_page_table
    bytes += huge_size;       // oversized active_transaction_table
    std::istringstream ss(bytes, std::istringstream::binary);
    LogRecord record;
    Decoder dec(ss);

    // Act + Assert -- same clean-rejection requirement.
    EXPECT_NO_THROW(dec >> record);  // FAILS today: std::length_error
    EXPECT_TRUE(record.active_transaction_table.empty());
  }
}

}  // namespace tinylamb