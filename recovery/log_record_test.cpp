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

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
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
  // Arrange -- a generic LogRecord for round-trip serdes, plus per-category
  // sub-arranges under SCOPED_TRACE
  SerializeDeserializeCheck(
      LogRecord(0xaabbccddeeff0011, 0x1122334455667788, LogType::kBegin));

  {
    // Arrange -- InsertBranch-related logs
    // (Inserting/InsertingLeaf/InsertingBranch + compensations)
    SCOPED_TRACE("Insertion log tests");
    // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality for
    // each log variant
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
    // Arrange -- UpdateBranch-related logs
    // (Updating/UpdatingLeaf/UpdatingBranch + compensations)
    SCOPED_TRACE("Updating log tests");
    // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality for
    // each log variant
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
    // Arrange -- Delete-related logs (Deleting/DeletingLeaf/DeletingBranch +
    // compensations)
    SCOPED_TRACE("Deletion log tests");
    // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality for
    // each log variant
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
    // Arrange -- IndexKey-related logs (SetLowFence / SetHighFence with various
    // key boundaries)
    SCOPED_TRACE("Fence log tests");
    // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality for
    // each fence log
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
    // Act + Assert -- SerializeDeserializeCheck asserts round-trip equality for
    // each foster log
    SerializeDeserializeCheck(LogRecord::SetFosterLogRecord(
        20, 21, 12, FosterPair("new", 0), FosterPair("old", 43)));
    SerializeDeserializeCheck(LogRecord::SetFosterLogRecord(
        2, 1, 11, FosterPair("ne", 44), FosterPair("old", 1)));
  }

  // Arrange -- Checkpoint-related logs (BeginCheckpoint / EndCheckpoint with
  // txn table) Act + Assert -- SerializeDeserializeCheck asserts round-trip
  // equality
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
  const std::vector<LogType> types{LogType::kUnknown,
                                   LogType::kBegin,
                                   LogType::kInsertRow,
                                   LogType::kInsertLeaf,
                                   LogType::kInsertBranch,
                                   LogType::kUpdateRow,
                                   LogType::kUpdateLeaf,
                                   LogType::kUpdateBranch,
                                   LogType::kDeleteRow,
                                   LogType::kDeleteLeaf,
                                   LogType::kDeleteBranch,
                                   LogType::kSetLowFence,
                                   LogType::kSetHighFence,
                                   LogType::kSetFoster,
                                   LogType::kCompensateInsertRow,
                                   LogType::kCompensateInsertLeaf,
                                   LogType::kCompensateInsertBranch,
                                   LogType::kCompensateUpdateRow,
                                   LogType::kCompensateUpdateLeaf,
                                   LogType::kCompensateUpdateBranch,
                                   LogType::kCompensateDeleteRow,
                                   LogType::kCompensateDeleteLeaf,
                                   LogType::kCompensateDeleteBranch,
                                   LogType::kCompensateSetLowFence,
                                   LogType::kCompensateSetHighFence,
                                   LogType::kCompensateSetFoster,
                                   LogType::kCommit,
                                   LogType::kBeginCheckpoint,
                                   LogType::kEndCheckpoint,
                                   LogType::kSystemAllocPage,
                                   LogType::kSystemDestroyPage,
                                   LogType::kLowestValue};

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
  // Deliberately out-of-range LogType to probe the "undefined" fallback arm
  // of operator<<.
  unknown << static_cast<LogType>(
      0xffff);  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
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
  LogRecord long_key = LogRecord::InsertingLeafLogRecord(
      12, 3, 1, std::string(100, 'x'), "redo");
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
  LogRecord lowest = LogRecord::CompensateSetLowestValueLogRecord(3, 1, 4);

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

  // Act + Assert -- foster and lowest round-trip through the decoder, along
  // with the two compensating fence types (kCompensateSetLowFence /
  // kCompensateSetHighFence now have decoder cases).
  SerializeDeserializeCheck(foster);
  SerializeDeserializeCheck(lowest);
  SerializeDeserializeCheck(high_fence);
}

TEST_F(LogRecordTest, CompensatingFenceRoundTrip) {
  // Regression: the decoder switch had no case for kCompensateSetLowFence /
  // kCompensateSetHighFence, so abort recovery of a fence-splitting
  // transaction failed to parse its own CLRs. Both must now survive a full
  // serialize -> deserialize round trip with redo_data intact.
  LogRecord low_fence =
      LogRecord::CompensateSetLowFenceLogRecord(42, 7, 9, IndexKey("low-key"));
  LogRecord high_fence = LogRecord::CompensateSetHighFenceLogRecord(
      43, 8, 10, IndexKey("high-key"));
  ASSERT_EQ(low_fence.Serialize().size(), low_fence.Size());
  ASSERT_EQ(high_fence.Serialize().size(), high_fence.Size());

  SerializeDeserializeCheck(low_fence);
  SerializeDeserializeCheck(high_fence);

  // Act -- decode manually to pin the restored fields.
  LogRecord parsed;
  std::istringstream ss(low_fence.Serialize(), std::istringstream::binary);
  Decoder dec(ss);
  dec >> parsed;

  // Assert -- type, position and redo payload match the encoded CLR.
  EXPECT_EQ(parsed.type, LogType::kCompensateSetLowFence);
  EXPECT_EQ(parsed.prev_lsn, 42);
  EXPECT_EQ(parsed.txn_id, 7);
  EXPECT_EQ(parsed.pid, 9);
  EXPECT_TRUE(parsed.key.empty());
  EXPECT_TRUE(parsed.undo_data.empty());
  EXPECT_EQ(parsed.redo_data, Encode(IndexKey("low-key")));
}

TEST_F(LogRecordTest, DecodeHugeEndCheckpointTableSizeRejected) {
  // Fuzzer regression (log_record_fuzzer): a kEndCheckpoint record whose
  // encoded dirty_page_table / active_transaction_table size field is huge
  // once made Decoder::operator>>(std::vector<T>&)
  // (common/decoder.hpp) call vec.resize(size) with no bound against the
  // remaining input bytes, triggering an unbounded allocation (OOM).
  // common/decoder.hpp now guards this via kMaxDecodedElements +
  // setstate(failbit), so decoding rejects the record cleanly: no throw and
  // both tables stay empty. This test pins that behavior.
  const auto kEndCheckpointPrefix = [] {
    std::ostringstream stream(std::ios::binary);
    Encoder encoder(stream);
    encoder << kSerdesMagic << kSerdesVersion
            << static_cast<uint16_t>(LogType::kEndCheckpoint)
            << uint64_t{0}  // prev_lsn
            << uint64_t{0}  // txn_id
            << uint8_t{0};  // types: no pid / slot / key
    return stream.str();
  }();
  const std::string huge_size(8, '\x20');  // 0x2020202020202020 elements

  // Arrange -- a kEndCheckpoint whose dirty_page_table claims a huge length.
  {
    std::istringstream ss(kEndCheckpointPrefix + huge_size,
                          std::istringstream::binary);
    LogRecord record;
    Decoder dec(ss);

    // Act + Assert -- the oversized length is rejected via failbit.
    EXPECT_NO_THROW(dec >> record);
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
    EXPECT_NO_THROW(dec >> record);
    EXPECT_TRUE(record.active_transaction_table.empty());
  }
}

TEST_F(LogRecordTest, DumpUnknownRecordType) {
  // Arrange -- a default-constructed record carries LogType::kUnknown.
  LogRecord unknown;

  // Act -- stream the record and its type.
  std::stringstream ss;
  ss << unknown;
  std::stringstream type_ss;
  type_ss << LogType::kUnknown;

  // Assert -- the type tag renders as "(unknown)" and the record dump still
  // prints the trailing LSN/txn metadata.
  EXPECT_EQ(type_ss.str(), "(unknown) ");
  EXPECT_NE(ss.str().find("(unknown)"), std::string::npos);
  EXPECT_NE(ss.str().find("prev_lsn"), std::string::npos);
}

TEST_F(LogRecordTest, SizeOfUnknownLogAborts) {
  // LogRecord::Size() rejects LogType::kUnknown loudly: abort via assert() in
  // debug builds, throw std::runtime_error under NDEBUG.  Either way the
  // caller must never observe a size for an undefined record layout.
#ifdef NDEBUG
  LogRecord unknown;
  EXPECT_THROW((void)unknown.Size(), std::runtime_error);
#else
  LogRecord unknown;
  EXPECT_DEATH((void)unknown.Size(), "unknown");
#endif
}

TEST_F(LogRecordTest, SerializeUnknownLogAborts) {
  // Serializing a kUnknown record trips an assert in debug builds and throws
  // std::runtime_error under NDEBUG; a garbage record must never be written.
#ifdef NDEBUG
  LogRecord unknown;
  EXPECT_THROW((void)unknown.Serialize(), std::runtime_error);
#else
  LogRecord unknown;
  EXPECT_DEATH((void)unknown.Serialize(), "unknown");
#endif
}

TEST_F(LogRecordTest, DecodeUnknownLogTypeThrowsCleanly) {
  // Arrange -- a byte stream whose type field is not a defined LogType.
  // The decoder rejects it with a catchable exception (never a half-record,
  // never an assert): RecoveryManager skips such torn tails and the fuzzers
  // treat this as ordinary rejection.
  std::string bytes;
  bytes.append(1, static_cast<char>(0xff))
      .append(1, static_cast<char>(0xff));  // uint16 LogType: 0xffff
  bytes.append(8, '\x00');                  // prev_lsn
  bytes.append(8, '\x00');                  // txn_id
  bytes.append(1, '\x00');                  // types: no pid / slot / key
  std::istringstream ss(bytes, std::istringstream::binary);

  // Act + Assert -- the decoder reaches its default arm and throws.
  LogRecord record;
  Decoder dec(ss);
  EXPECT_THROW(dec >> record, std::runtime_error);
}

TEST_F(LogRecordTest, ThreeArgConstructorSetsFields) {
  // Arrange + Act -- construct with explicit lsn / txn / type.
  const LogRecord l(0xaabbccddeeff0011, 0x1122334455667788, LogType::kBegin);

  // Assert -- the three keyed fields are stored verbatim.
  EXPECT_EQ(l.type, LogType::kBegin);
  EXPECT_EQ(l.prev_lsn, 0xaabbccddeeff0011);
  EXPECT_EQ(l.txn_id, 0x1122334455667788);
}

TEST_F(LogRecordTest, ClearResetsAllFields) {
  // Arrange -- a record of the widest category with every field populated.
  LogRecord log = LogRecord::EndCheckpointLogRecord(
      {{1, 2}, {3, 4}}, {{5, TransactionStatus::kCommitted, 6}});
  log.pid = 123;
  log.slot = 7;
  log.key = "key";
  log.undo_data = "undo";
  log.redo_data = "redo";
  log.redo_page = 5;
  log.undo_page = 6;
  log.allocated_page_type = PageType::kRowPage;

  // Act -- reset the record to its empty state.
  log.Clear();

  // Assert -- every field returns to its documented default.
  EXPECT_EQ(log.type, LogType::kUnknown);
  EXPECT_EQ(log.prev_lsn, 0);
  EXPECT_EQ(log.txn_id, 0);
  EXPECT_EQ(log.pid, std::numeric_limits<page_id_t>::max());
  EXPECT_EQ(log.slot, std::numeric_limits<slot_t>::max());
  EXPECT_TRUE(log.key.empty());
  EXPECT_TRUE(log.undo_data.empty());
  EXPECT_TRUE(log.redo_data.empty());
  EXPECT_EQ(log.redo_page, 0);
  EXPECT_EQ(log.undo_page, 0);
  EXPECT_TRUE(log.dirty_page_table.empty());
  EXPECT_TRUE(log.active_transaction_table.empty());
  EXPECT_EQ(log.allocated_page_type, PageType::kUnknown);
  EXPECT_FALSE(log.HasPageID());
  EXPECT_FALSE(log.HasSlot());
}

TEST_F(LogRecordTest, RoundTripAllRecordKindsWithSize) {
  // One instance of every factory-built record kind, round-tripped through
  // Serialize()/Decoder with Size() matching the byte count each time.
  const std::vector<LogRecord> records{
      LogRecord(0xaabbccddeeff0011, 0x1122334455667788, LogType::kBegin),
      LogRecord(0xaabbccddeeff0011, 0x1122334455667788, LogType::kCommit),
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
      LogRecord::CompensateSetLowFenceLogRecord(2, 3, 4, IndexKey("redo")),
      LogRecord::CompensateSetHighFenceLogRecord(2, 3, 4, IndexKey("redo")),
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
          {{1, 2}, {3, 4}}, {{3, TransactionStatus::kCommitted, 4}}),
  };

  // Act + Assert -- each record round-trips byte-exactly and Size() is exact.
  for (const auto& log : records) {
    SCOPED_TRACE(static_cast<uint16_t>(log.type));
    SerializeDeserializeCheck(log);
    EXPECT_EQ(log.Serialize().size(), log.Size());
  }
}

TEST_F(LogRecordTest, DumpPositionVariants) {
  // Arrange -- records whose position renders from page-id only, slot only, or
  // key only, so every branch of DumpPosition is visible in the output.
  LogRecord page_only = LogRecord::BeginCheckpointLogRecord();
  page_only.pid = 7;
  LogRecord slot_only = LogRecord::CompensatingInsertLogRecord(3, 0, 9);
  slot_only.pid = std::numeric_limits<page_id_t>::max();
  LogRecord key_only = LogRecord::CompensatingInsertLogRecord(3, 0, "the-key");

  // Act -- render each position.
  std::stringstream page_ss;
  page_only.DumpPosition(page_ss);
  std::stringstream slot_ss;
  slot_only.DumpPosition(slot_ss);
  std::stringstream key_ss;
  key_only.DumpPosition(key_ss);

  // Assert -- the page/slot/key selectors print exactly their own fields.
  EXPECT_NE(page_ss.str().find("Page: 7"), std::string::npos);
  EXPECT_EQ(page_ss.str().find('|'), std::string::npos);
  EXPECT_NE(slot_ss.str().find("| 9"), std::string::npos);
  EXPECT_NE(key_ss.str().find("the-key"), std::string::npos);
}

TEST_F(LogRecordTest, V2RecordsWithoutCrcStillDecode) {
  // Writers emit v3 (with CRC); readers must keep accepting v1/v2 (no CRC)
  // per log_record.hpp. Re-encode a kBegin record as v2 by stripping the
  // trailing CRC and patching the big-endian version field (bytes [4, 8)).
  const LogRecord begin(1, 2, LogType::kBegin);
  std::string v3 = begin.Serialize();
  ASSERT_GE(v3.size(), 4 + 4 + kWalRecordCrcSize);
  std::string v2 = v3.substr(0, v3.size() - kWalRecordCrcSize);
  ASSERT_EQ(static_cast<unsigned char>(v2[7]), 3);
  v2[7] = static_cast<char>(2);
  std::istringstream in(v2, std::istringstream::binary);
  Decoder dec(in);
  LogRecord decoded;
  EXPECT_NO_THROW(dec >> decoded);
  EXPECT_EQ(decoded.type, LogType::kBegin);
  EXPECT_EQ(decoded.wire_version, 2U);
}

TEST_F(LogRecordTest, EqualityIsFieldSensitive) {
  // Arrange -- two EndCheckpoint records that differ only in one table entry.
  LogRecord a = LogRecord::EndCheckpointLogRecord(
      {{1, 2}}, {{3, TransactionStatus::kCommitted, 4}});
  LogRecord b = LogRecord::EndCheckpointLogRecord(
      {{1, 99}}, {{3, TransactionStatus::kCommitted, 4}});

  // Act + Assert -- operator== compares every field, so the LSN difference in
  // the dirty-page table makes the records unequal while both serialize.
  EXPECT_NE(a, b);
  EXPECT_EQ(a.Serialize().size(), a.Size());
  EXPECT_EQ(b.Serialize().size(), b.Size());
}

}  // namespace tinylamb

// Tests below mirror the log_record_fuzzer oracle (recovery/
// log_record_fuzzer.hpp): decoding arbitrary bytes must never throw for a
// defined LogType, and every successfully decoded record must round-trip
// byte-stably through Serialize/Deserialize.
namespace tinylamb {
namespace {

constexpr uint8_t kMaskPageID = 0x1;
constexpr uint8_t kMaskSlot = 0x2;
constexpr uint8_t kMaskKey = 0x4;

// Builds a record payload for |type| with the optional fields selected by
// |types_mask| present, |body_len| filler bytes where type-specific data
// would go, truncated to |truncate_to| bytes overall.
std::string FuzzShapedRecord(LogType type, uint8_t types_mask, size_t body_len,
                             size_t truncate_to) {
  std::string bytes;
  const auto raw = static_cast<uint16_t>(type);
  bytes.append(reinterpret_cast<const char*>(&raw), sizeof(raw));
  bytes.append(8, '\x01');  // prev_lsn
  bytes.append(8, '\x02');  // txn_id
  bytes.append(1, static_cast<char>(types_mask));
  if ((types_mask & kMaskPageID) != 0) {
    bytes.append(4, '\x03');
  }
  if ((types_mask & kMaskSlot) != 0) {
    bytes.append(2, '\x04');
  }
  if ((types_mask & kMaskKey) != 0) {
    const uint64_t key_len = 3;
    bytes.append(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
    bytes.append("abc");
  }
  bytes.append(body_len, '\x7f');
  bytes.resize(std::min(truncate_to, bytes.size()));
  return bytes;
}

void RoundTripMustBeStable(const std::string& input) {
  LogRecord first;
  {
    std::istringstream ss(input, std::istringstream::binary);
    Decoder dec(ss);
    // A throw here is ordinary rejection (truncation can zero out the type
    // field, undefined types throw); the fuzzer treats it the same way.
    try {
      dec >> first;
    } catch (const std::exception&) {
      return;
    }
  }
  // Undefined types never reach this point; nothing to round-trip otherwise.
  if (first.type == LogType::kUnknown) {
    return;
  }
  const std::string serialized = first.Serialize();
  LogRecord second;
  {
    std::istringstream ss(serialized, std::istringstream::binary);
    Decoder dec(ss);
    ASSERT_NO_THROW(dec >> second);
  }
  EXPECT_EQ(second.Serialize(), serialized);
}

}  // namespace

TEST_F(LogRecordTest, TruncatedRecordsDecodeAndRoundTripStably) {
  // Every (record kind x field-mask x truncation point) combination behaves
  // like the fuzzer expects: clean decode of defined types and byte-stable
  // re-encode of whatever was decoded.
  const std::array<LogType, 13> kinds = {
      LogType::kBegin,         LogType::kCommit,
      LogType::kInsertRow,     LogType::kInsertLeaf,
      LogType::kInsertBranch,  LogType::kUpdateRow,
      LogType::kDeleteRow,     LogType::kUpdateBranch,
      LogType::kSetFoster,     LogType::kCompensateInsertRow,
      LogType::kEndCheckpoint, LogType::kSystemAllocPage,
      LogType::kLowestValue,
  };
  const std::array<uint8_t, 4> masks = {
      static_cast<uint8_t>(0x00),
      kMaskPageID,
      static_cast<uint8_t>(kMaskPageID | kMaskSlot),
      static_cast<uint8_t>(kMaskPageID | kMaskSlot | kMaskKey),
  };
  for (const LogType kind : kinds) {
    for (const uint8_t mask : masks) {
      for (const size_t cut : {size_t{0}, size_t{9}, size_t{17}, size_t{21},
                               size_t{23}, size_t{40}}) {
        SCOPED_TRACE(testing::Message()
                     << "type=" << static_cast<int>(kind)
                     << " mask=" << static_cast<int>(mask) << " cut=" << cut);
        RoundTripMustBeStable(FuzzShapedRecord(kind, mask, 16, cut));
      }
    }
  }
}

TEST_F(LogRecordTest, UndefinedLogTypeInTruncatedTailThrowsCleanly) {
  // An out-of-range type value must never return a half-record: the decoder
  // rejects the torn tail with a catchable exception in every build type.
  std::string bytes;
  const uint16_t raw = 0xffff;  // not a defined LogType
  bytes.append(reinterpret_cast<const char*>(&raw), sizeof(raw));
  bytes.append(17, '\x00');
  std::istringstream ss(bytes, std::istringstream::binary);
  LogRecord record;
  Decoder dec(ss);
  EXPECT_THROW(dec >> record, std::runtime_error);
}

}  // namespace tinylamb
