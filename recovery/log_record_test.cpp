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
#include <cstring>
#include <exception>
#include <ios>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "common/constants.hpp"
#include "common/crc32c.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/serdes.hpp"
#include "gtest/gtest.h"
#include "page/index_key.hpp"
#include "page/page_type.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {
namespace {

std::string ReferenceSerialize(const LogRecord& record) {
  std::stringstream stream;
  Encoder encoder(stream);
  encoder << record;
  std::string bytes = stream.str();
  const uint32_t crc = Crc32C(bytes.data(), bytes.size());
  const std::array<char, sizeof(uint32_t)> crc_bytes{
      static_cast<char>((crc >> 24) & 0xFF),
      static_cast<char>((crc >> 16) & 0xFF),
      static_cast<char>((crc >> 8) & 0xFF), static_cast<char>(crc & 0xFF)};
  bytes.append(crc_bytes.data(), crc_bytes.size());
  return bytes;
}

LogRecord TestAllocation(lsn_t prev_lsn, txn_id_t txn, page_id_t pid,
                         PageType page_type,
                         PageAllocationKind kind = PageAllocationKind::kFresh) {
  if (kind == PageAllocationKind::kFresh) {
    return LogRecord::AllocatePageLogRecord(
        prev_lsn, txn, pid, page_type, kind, PageType::kFreePage, {},
        EncodeAllocationMetaPayload(0, pid - 1));
  }
  if (kind == PageAllocationKind::kRecycled) {
    return LogRecord::AllocatePageLogRecord(
        prev_lsn, txn, pid, page_type, kind, PageType::kFreePage,
        EncodeAllocationFreePayload(0, txn),
        EncodeAllocationMetaPayload(pid, pid));
  }
  return LogRecord::AllocatePageLogRecord(
      prev_lsn, txn, pid, page_type, kind, PageType::kRowPage,
      std::string(kPageBodySize, 'a'), EncodeAllocationMetaPayload(0, pid));
}
}  // namespace

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
  SerializeDeserializeCheck(TestAllocation(15, 7, 10, PageType::kRowPage));
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
      LogRecord::SetRowPageNextLogRecord(1, 2, 3, 4, 5),
      LogRecord::SetRowPagePrevLogRecord(1, 2, 3, 4, 5),
      LogRecord::CompensateSetRowPageNextLogRecord(1, 2, 3, 4),
      LogRecord::CompensateSetRowPagePrevLogRecord(1, 2, 3, 4),
      TestAllocation(1, 2, 3, PageType::kRowPage),
      LogRecord::CompensateAllocatePageLogRecord(
          TestAllocation(1, 2, 3, PageType::kRowPage)),
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

TEST_F(LogRecordTest, SerializeToMatchesSerializeForRepresentativeRecords) {
  const std::string embedded("a\0b", 3);
  const std::vector<LogRecord> records{
      LogRecord(1, 2, LogType::kBegin),
      LogRecord::InsertingLeafLogRecord(3, 4, 5, std::string("k\0", 2),
                                        embedded),
      LogRecord::UpdatingLeafLogRecord(6, 7, 8, "key", {}, embedded),
      LogRecord::DestroyPageLogRecord(9, 10, 11, PageType::kRowPage, embedded),
      LogRecord::CompensateDestroyPageLogRecord(12, 13, PageType::kLeafPage,
                                                embedded),
      LogRecord::FullPageImageLogRecord(14, PageType::kRowPage, embedded),
      LogRecord::EndCheckpointLogRecord({{1, 2}, {3, 4}},
                                        {{5, TransactionStatus::kRunning, 6}}),
  };

  for (const LogRecord& record : records) {
    SCOPED_TRACE(static_cast<uint16_t>(record.type));
    const std::string expected = ReferenceSerialize(record);
    EXPECT_EQ(record.Serialize(), expected);
    std::string direct;
    record.SerializeTo(direct);
    EXPECT_EQ(direct, expected);

    std::string appended = "prefix";
    record.SerializeTo(appended);
    EXPECT_EQ(appended, "prefix" + expected);
  }
}

TEST_F(LogRecordTest, SerializeToWritesExactV6HeaderAndCrc) {
  const LogRecord record(0x0102030405060708ULL, 0x1112131415161718ULL,
                         LogType::kBegin);
  static constexpr std::array<char, 32> expected_bytes = {
      "\x54\x59\x42\x31"
      "\x00\x00\x00\x06"
      "\x00\x01"
      "\x01\x02\x03\x04\x05\x06\x07\x08"
      "\x11\x12\x13\x14\x15\x16\x17\x18"
      "\x00"
      "\xa7\x9c\x60\xbe"};
  static_assert(expected_bytes.size() == 32);
  const std::string expected(expected_bytes.data(), expected_bytes.size() - 1);

  std::string actual;
  record.SerializeTo(actual);
  EXPECT_EQ(actual, expected);

  std::string appended = "p";
  record.SerializeTo(appended);
  EXPECT_EQ(appended, std::string("p") + expected);
}

TEST_F(LogRecordTest, RowPageLinkRecordsRoundTrip) {
  const std::vector<LogRecord> records{
      LogRecord::SetRowPageNextLogRecord(11, 22, 33, 44, 55),
      LogRecord::SetRowPagePrevLogRecord(12, 23, 34, 45, 56),
      LogRecord::CompensateSetRowPageNextLogRecord(13, 24, 35, 57),
      LogRecord::CompensateSetRowPagePrevLogRecord(14, 25, 36, 58),
  };
  for (const LogRecord& record : records) {
    EXPECT_EQ(record.Serialize().size(), record.Size());
    SerializeDeserializeCheck(record);
  }
}

TEST_F(LogRecordTest, VersionFourRecordStillDecodes) {
  const LogRecord source = LogRecord::SetLowestLogRecord(1, 2, 3, 4, 5);
  std::string v4 = source.Serialize();
  SerializeU32(v4.data() + sizeof(uint32_t), 4);
  const size_t body_size = v4.size() - kWalRecordCrcSize;
  const uint32_t crc = Crc32C(v4.data(), body_size);
  v4[body_size] = static_cast<char>((crc >> 24) & 0xff);
  v4[body_size + 1] = static_cast<char>((crc >> 16) & 0xff);
  v4[body_size + 2] = static_cast<char>((crc >> 8) & 0xff);
  v4[body_size + 3] = static_cast<char>(crc & 0xff);

  std::istringstream input(v4, std::istringstream::binary);
  Decoder decoder(input);
  LogRecord decoded;
  decoder >> decoded;
  ASSERT_FALSE(decoder.Failed());
  EXPECT_EQ(decoded.wire_version, 4U);
  EXPECT_EQ(decoded.type, LogType::kLowestValue);
  EXPECT_EQ(decoded.redo_page, 4U);
  EXPECT_EQ(decoded.undo_page, 5U);
}

TEST_F(LogRecordTest, VersionSixAllocationPayloadRoundTrips) {
  for (const PageAllocationKind kind :
       {PageAllocationKind::kFresh, PageAllocationKind::kRecycled,
        PageAllocationKind::kInPlace}) {
    LogRecord record = TestAllocation(91, 17, 23, PageType::kRowPage, kind);
    EXPECT_EQ(record.redo_data.size(), kAllocationMetaPayloadSize);
    if (kind == PageAllocationKind::kFresh) {
      EXPECT_TRUE(record.undo_data.empty());
      EXPECT_LT(record.Size(), 1024U);
    } else if (kind == PageAllocationKind::kRecycled) {
      EXPECT_EQ(record.undo_data.size(), kAllocationFreePayloadSize);
      EXPECT_LT(record.Size(), 1024U);
    } else {
      EXPECT_EQ(record.undo_data.size(), kPageBodySize);
      EXPECT_GE(record.Size(), kPageBodySize);
      EXPECT_LT(record.Size(), kPageBodySize + 1024U);
      EXPECT_EQ(record.undo_data.size(), kPageBodySize);
      record.undo_data.front() = '\0';
      record.undo_data.back() = '\x7f';
    }
    page_id_t old_first = 0;
    page_id_t old_max = 0;
    ASSERT_TRUE(
        DecodeAllocationMetaPayload(record.redo_data, &old_first, &old_max));
    EXPECT_LE(old_first, old_max);
    ASSERT_EQ(record.Serialize().size(), record.Size());
    EXPECT_EQ(static_cast<unsigned char>(record.Serialize()[7]),
              kWalRecordVersion);
    SerializeDeserializeCheck(record);

    const LogRecord clr =
        LogRecord::CompensateAllocatePageLogRecord(record, 55);
    EXPECT_EQ(clr.type, LogType::kCompensateAllocPage);
    EXPECT_EQ(clr.prev_lsn, 55U);
    EXPECT_EQ(clr.allocation_kind, kind);
    EXPECT_EQ(clr.undo_data, record.undo_data);
    EXPECT_EQ(clr.redo_data, record.redo_data);
    EXPECT_EQ(clr.Serialize().size(), clr.Size());
    SerializeDeserializeCheck(clr);
  }
}

TEST_F(LogRecordTest, AllocationPayloadUsesStableBigEndianLayout) {
  EXPECT_EQ(
      EncodeAllocationMetaPayload(0x0102030405060708ULL, 0x1112131415161718ULL),
      std::string("\x01\x02\x03\x04\x05\x06\x07\x08"
                  "\x11\x12\x13\x14\x15\x16\x17\x18",
                  16));
  EXPECT_EQ(
      EncodeAllocationFreePayload(0x2122232425262728ULL, 0x3132333435363738ULL),
      std::string("\x21\x22\x23\x24\x25\x26\x27\x28"
                  "\x31\x32\x33\x34\x35\x36\x37\x38",
                  16));

  page_id_t first_free = 0;
  page_id_t max_page = 0;
  ASSERT_TRUE(DecodeAllocationMetaPayload(EncodeAllocationMetaPayload(41, 73),
                                          &first_free, &max_page));
  EXPECT_EQ(first_free, 41U);
  EXPECT_EQ(max_page, 73U);

  page_id_t next_free = 0;
  txn_id_t destroyer = 0;
  ASSERT_TRUE(DecodeAllocationFreePayload(EncodeAllocationFreePayload(17, 91),
                                          &next_free, &destroyer));
  EXPECT_EQ(next_free, 17U);
  EXPECT_EQ(destroyer, 91U);
  EXPECT_FALSE(DecodeAllocationMetaPayload({}, &first_free, &max_page));
  EXPECT_FALSE(DecodeAllocationFreePayload(std::string(15, '\0'), &next_free,
                                           &destroyer));

  const LogRecord repaired_high_water = LogRecord::AllocatePageLogRecord(
      0, 1, 3, PageType::kRowPage, PageAllocationKind::kRecycled,
      PageType::kFreePage, EncodeAllocationFreePayload(5, 1),
      EncodeAllocationMetaPayload(3, 2));
  EXPECT_EQ(repaired_high_water.redo_data, EncodeAllocationMetaPayload(3, 2));
  SerializeDeserializeCheck(repaired_high_water);

  const LogRecord forced_fresh = LogRecord::AllocatePageLogRecord(
      0, 1, 1, PageType::kLeafPage, PageAllocationKind::kFresh,
      PageType::kFreePage, {}, EncodeAllocationMetaPayload(1, 0));
  EXPECT_TRUE(forced_fresh.undo_data.empty());
  SerializeDeserializeCheck(forced_fresh);
}

TEST_F(LogRecordTest, VersionSixAllocationDecoderRejectsMalformedPayloads) {
  const auto expect_rejected = [](const LogRecord& record) {
    std::istringstream input(record.Serialize(), std::istringstream::binary);
    Decoder decoder(input);
    LogRecord decoded;
    decoder >> decoded;
    EXPECT_TRUE(decoder.Failed());
  };

  LogRecord fresh =
      TestAllocation(1, 2, 3, PageType::kRowPage, PageAllocationKind::kFresh);
  LogRecord bad_fresh_undo = fresh;
  bad_fresh_undo.undo_data.push_back('\0');
  expect_rejected(bad_fresh_undo);

  LogRecord bad_fresh_meta = fresh;
  bad_fresh_meta.redo_data = EncodeAllocationMetaPayload(0, 3);
  expect_rejected(bad_fresh_meta);

  LogRecord recycled = TestAllocation(1, 2, 3, PageType::kRowPage,
                                      PageAllocationKind::kRecycled);
  LogRecord bad_recycled_meta = recycled;
  bad_recycled_meta.redo_data = EncodeAllocationMetaPayload(0, 3);
  expect_rejected(bad_recycled_meta);

  LogRecord bad_recycled_link = recycled;
  bad_recycled_link.undo_data = EncodeAllocationFreePayload(3, 2);
  expect_rejected(bad_recycled_link);

  LogRecord in_place =
      TestAllocation(1, 2, 3, PageType::kRowPage, PageAllocationKind::kInPlace);
  LogRecord bad_in_place_body = in_place;
  bad_in_place_body.undo_data.pop_back();
  expect_rejected(bad_in_place_body);

  LogRecord bad_in_place_meta = in_place;
  bad_in_place_meta.previous_page_type = PageType::kMetaPage;
  expect_rejected(bad_in_place_meta);

  LogRecord bad_target_type = fresh;
  bad_target_type.allocated_page_type = PageType::kMetaPage;
  expect_rejected(bad_target_type);

  LogRecord bad_kind = fresh;
  // Write an out-of-range discriminant without naming it as the enum (the
  // analyzer rightly rejects casting 3 to PageAllocationKind directly).
  std::underlying_type_t<PageAllocationKind> invalid_kind = 3;
  std::memcpy(&bad_kind.allocation_kind, &invalid_kind, sizeof(invalid_kind));
  expect_rejected(bad_kind);
}

TEST_F(LogRecordTest, VersionOneThroughFiveAllocationRecordsDecode) {
  const LogRecord current = TestAllocation(3, 4, 5, PageType::kLeafPage);
  for (uint32_t version = kLegacyWalRecordVersion;
       version < kAllocationPayloadWalVersion; ++version) {
    std::string bytes = current.Serialize();
    bytes.resize(bytes.size() - (sizeof(uint64_t) + sizeof(uint8_t) +
                                 SerializeSize(current.undo_data) +
                                 SerializeSize(current.redo_data)));
    if (version < kCrcWalRecordVersion) {
      bytes.resize(bytes.size() - kWalRecordCrcSize);
    }
    SerializeU32(bytes.data() + sizeof(uint32_t), version);
    if (version >= kCrcWalRecordVersion) {
      const size_t body_size = bytes.size() - kWalRecordCrcSize;
      const uint32_t crc = Crc32C(bytes.data(), body_size);
      bytes[body_size] = static_cast<char>((crc >> 24) & 0xff);
      bytes[body_size + 1] = static_cast<char>((crc >> 16) & 0xff);
      bytes[body_size + 2] = static_cast<char>((crc >> 8) & 0xff);
      bytes[body_size + 3] = static_cast<char>(crc & 0xff);
    }

    std::istringstream input(bytes, std::istringstream::binary);
    Decoder decoder(input);
    LogRecord decoded;
    decoder >> decoded;
    ASSERT_FALSE(decoder.Failed()) << version;
    EXPECT_EQ(decoded.type, LogType::kSystemAllocPage);
    EXPECT_EQ(decoded.wire_version, version);
    EXPECT_EQ(decoded.allocated_page_type, PageType::kLeafPage);
    EXPECT_EQ(decoded.previous_page_type, PageType::kUnknown);
    EXPECT_EQ(decoded.allocation_kind, PageAllocationKind::kFresh);
    EXPECT_TRUE(decoded.undo_data.empty());
    EXPECT_TRUE(decoded.redo_data.empty());
    EXPECT_EQ(decoded.Size(), bytes.size());

    const std::string reserialized = decoded.Serialize();
    EXPECT_EQ(reserialized.size(), bytes.size());
    std::istringstream reserialized_input(reserialized,
                                          std::istringstream::binary);
    Decoder reserialized_decoder(reserialized_input);
    LogRecord reparsed;
    reserialized_decoder >> reparsed;
    ASSERT_FALSE(reserialized_decoder.Failed()) << version;
    EXPECT_EQ(reparsed.wire_version, version);
    EXPECT_EQ(reparsed.Size(), reserialized.size());
  }
}

TEST_F(LogRecordTest, SerializeToUpgradesDecodedLegacyRecords) {
  const LogRecord source = LogRecord::DestroyPageLogRecord(
      17, 18, 19, PageType::kRowPage, std::string("body\0", 5));
  for (const uint32_t version : {kLegacyWalRecordVersion, 2U}) {
    std::string legacy = source.Serialize();
    if (version == kLegacyWalRecordVersion) {
      legacy.resize(legacy.size() - sizeof(uint64_t) -
                    SerializeSize(source.undo_data) - kWalRecordCrcSize);
    } else {
      legacy.resize(legacy.size() - kWalRecordCrcSize);
    }
    SerializeU32(legacy.data() + 4, version);

    std::istringstream input(legacy, std::istringstream::binary);
    Decoder decoder(input);
    LogRecord decoded;
    decoder >> decoded;
    ASSERT_FALSE(decoder.Failed());
    EXPECT_EQ(decoded.wire_version, version);
    EXPECT_EQ(decoded.Size(), legacy.size());

    const std::string expected = ReferenceSerialize(decoded);
    EXPECT_EQ(decoded.Serialize(), expected);
    std::string output;
    decoded.SerializeTo(output);
    EXPECT_EQ(output, expected);
    ASSERT_GT(output.size(), kWalRecordCrcSize);
    EXPECT_EQ(static_cast<unsigned char>(output[7]), kWalRecordVersion);
    const size_t body_size = output.size() - kWalRecordCrcSize;
    const size_t crc_start = body_size;
    const uint32_t stored_crc =
        (static_cast<uint32_t>(static_cast<unsigned char>(output[crc_start]))
         << 24) |
        (static_cast<uint32_t>(
             static_cast<unsigned char>(output[crc_start + 1]))
         << 16) |
        (static_cast<uint32_t>(
             static_cast<unsigned char>(output[crc_start + 2]))
         << 8) |
        static_cast<uint32_t>(
            static_cast<unsigned char>(output[crc_start + 3]));
    EXPECT_EQ(stored_crc, Crc32C(output.data(), body_size));
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
                                   LogType::kCompensateSetLowestValue,
                                   LogType::kSetRowPageNext,
                                   LogType::kSetRowPagePrev,
                                   LogType::kCompensateSetRowPageNext,
                                   LogType::kCompensateSetRowPagePrev,
                                   LogType::kCommit,
                                   LogType::kBeginCheckpoint,
                                   LogType::kEndCheckpoint,
                                   LogType::kSystemAllocPage,
                                   LogType::kSystemDestroyPage,
                                   LogType::kLowestValue,
                                   LogType::kCompensateDestroyPage,
                                   LogType::kCompensateAllocPage};

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
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  const auto unknown_log_type = static_cast<LogType>(0xffff);
  unknown << unknown_log_type;
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
      TestAllocation(12, 3, 1, PageType::kLeafPage),
      LogRecord::CompensateAllocatePageLogRecord(
          TestAllocation(12, 3, 1, PageType::kLeafPage)),
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
  EXPECT_EQ(lowest.type, LogType::kCompensateSetLowestValue);
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

TEST_F(LogRecordTest, CompensationCarriesUndoNextLsn) {
  const lsn_t undo_next = 0x123456789abcdef0ULL;
  EXPECT_EQ(
      LogRecord::CompensatingInsertLogRecord(7, 11, 3, undo_next).prev_lsn,
      undo_next);
  EXPECT_EQ(
      LogRecord::CompensatingInsertLogRecord(7, 11, "k", undo_next).prev_lsn,
      undo_next);
  EXPECT_EQ(LogRecord::CompensatingInsertBranchLogRecord(7, 11, "k", undo_next)
                .prev_lsn,
            undo_next);
  EXPECT_EQ(LogRecord::CompensatingUpdateLogRecord(7, 11, 3, "redo", undo_next)
                .prev_lsn,
            undo_next);
  EXPECT_EQ(
      LogRecord::CompensatingUpdateLeafLogRecord(7, 11, "k", "redo", undo_next)
          .prev_lsn,
      undo_next);
  EXPECT_EQ(
      LogRecord::CompensatingUpdateBranchLogRecord(7, 11, "k", 9, undo_next)
          .prev_lsn,
      undo_next);
  EXPECT_EQ(LogRecord::CompensatingDeleteLogRecord(7, 11, 3, "redo", undo_next)
                .prev_lsn,
            undo_next);
  EXPECT_EQ(
      LogRecord::CompensatingDeleteLeafLogRecord(7, 11, "k", "redo", undo_next)
          .prev_lsn,
      undo_next);
  EXPECT_EQ(
      LogRecord::CompensatingDeleteBranchLogRecord(7, 11, "k", 9, undo_next)
          .prev_lsn,
      undo_next);
  EXPECT_EQ(LogRecord::CompensateSetLowestValueLogRecord(7, 11, 9, undo_next)
                .prev_lsn,
            undo_next);
  EXPECT_EQ(LogRecord::CompensateSetRowPageNextLogRecord(undo_next, 7, 11, 9)
                .prev_lsn,
            undo_next);
  EXPECT_EQ(LogRecord::CompensateSetRowPagePrevLogRecord(undo_next, 7, 11, 9)
                .prev_lsn,
            undo_next);
  EXPECT_EQ(LogRecord::CompensateDestroyPageLogRecord(7, 11, PageType::kRowPage,
                                                      "body", undo_next)
                .prev_lsn,
            undo_next);
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
  // LogRecord::Size() rejects LogType::kUnknown loudly (CHECK): the caller
  // must never observe a size for an undefined record layout.
  LogRecord unknown;
  EXPECT_DEATH((void)unknown.Size(), "unknown");
}

TEST_F(LogRecordTest, SerializeUnknownLogAborts) {
  // Serializing a kUnknown record trips CHECK in every build; a garbage
  // record must never be written.
  LogRecord unknown;
  EXPECT_DEATH((void)unknown.Serialize(), "unknown");
}

TEST_F(LogRecordTest, DecodeUnknownLogTypeFailsCleanly) {
  // Arrange -- a byte stream whose type field is not a defined LogType.
  // The decoder rejects it through the sticky fail state (never a
  // half-record, never an assert): RecoveryManager skips such torn tails and
  // the fuzzers treat this as ordinary rejection.
  std::string bytes;
  bytes.append(1, static_cast<char>(0xff))
      .append(1, static_cast<char>(0xff));  // uint16 LogType: 0xffff
  bytes.append(8, '\x00');                  // prev_lsn
  bytes.append(8, '\x00');                  // txn_id
  bytes.append(1, '\x00');                  // types: no pid / slot / key
  std::istringstream ss(bytes, std::istringstream::binary);

  // Act + Assert -- the decoder reaches its default arm and fails the stream.
  LogRecord record;
  Decoder dec(ss);
  dec >> record;
  EXPECT_TRUE(dec.Failed());
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
      LogRecord::SetRowPageNextLogRecord(1, 2, 3, 4, 5),
      LogRecord::SetRowPagePrevLogRecord(1, 2, 3, 4, 5),
      LogRecord::CompensateSetRowPageNextLogRecord(1, 2, 3, 4),
      LogRecord::CompensateSetRowPagePrevLogRecord(1, 2, 3, 4),
      TestAllocation(1, 2, 3, PageType::kRowPage),
      LogRecord::CompensateAllocatePageLogRecord(
          TestAllocation(1, 2, 3, PageType::kRowPage)),
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
  ASSERT_EQ(static_cast<unsigned char>(v2[7]), kWalRecordVersion);
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

TEST_F(LogRecordTest, UndefinedLogTypeInTruncatedTailFailsCleanly) {
  // An out-of-range type value must never return a half-record: the decoder
  // rejects the torn tail through the sticky fail state in every build.
  std::string bytes;
  const uint16_t raw = 0xffff;  // not a defined LogType
  bytes.append(reinterpret_cast<const char*>(&raw), sizeof(raw));
  bytes.append(17, '\x00');
  std::istringstream ss(bytes, std::istringstream::binary);
  LogRecord record;
  Decoder dec(ss);
  dec >> record;
  EXPECT_TRUE(dec.Failed());
}

}  // namespace tinylamb
