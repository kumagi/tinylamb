/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "type/row_block_codec.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <numbers>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/spill/spill_store.hpp"

namespace tinylamb {
namespace {

TEST(RowBlockCodecTest, RoundTripsAllValueVariantsIncludingLargeStrings) {
  const std::string huge(80000, 'Z');  // > 64 KiB (exceeds Encoder 16-bit max)
  std::vector<Row> rows;
  Value tagged_null;
  ASSERT_TRUE(tagged_null.SetCollation(2));
  rows.push_back(
      Row({tagged_null, Value(int64_t{-42}), Value(std::string(huge)),
           Value(std::numbers::pi), Value::DateFromDays(19724)}));
  rows.push_back(
      Row({Value(int64_t{7}).WithUnsigned(), Value("AbC").WithCollation(2),
           Value("1 day").WithInterval(),
           Value::Array({Value(int64_t{10}), Value(), Value(int64_t{20})},
                        "INT64")}));

  std::string encoded;
  ASSERT_EQ(EncodeRowBlock(rows, &encoded), Status::kSuccess);

  std::string_view sv(encoded);
  std::vector<Row> decoded;
  ASSERT_EQ(DecodeRowBlock(&sv, &decoded), Status::kSuccess);
  EXPECT_TRUE(sv.empty());
  ASSERT_EQ(decoded.size(), rows.size());
  EXPECT_EQ(decoded[0], rows[0]);
  EXPECT_EQ(decoded[0][0].Collation(), 2);
  EXPECT_EQ(decoded[1], rows[1]);
  EXPECT_EQ(decoded[1][0].Collation(), rows[1][0].Collation());
  EXPECT_TRUE(decoded[1][0].IsUnsigned());
  EXPECT_EQ(decoded[1][1].Collation(), 2);
  EXPECT_TRUE(decoded[1][2].IsInterval());
  EXPECT_EQ(decoded[1][3].ArrayElements().size(), 3U);
  EXPECT_EQ(decoded[1][3].ArrayElementSqlType(), "INT64");
}

// CheckSerializable (row-page/persist path) and the spill codec must accept
// exactly the same maximum array nesting depth; otherwise a value that passes
// validation can still fail when it is spilled.
TEST(RowBlockCodecTest, ArrayNestingDepthBoundaryAgreesWithPersistenceLimit) {
  auto nested = [](int levels) {
    Value v(int64_t{7});
    for (int i = 0; i < levels; ++i) {
      v = Value::Array({v}, "INT64");
    }
    return v;
  };

  const int limit = static_cast<int>(Value::kMaxArrayNestingDepth);
  for (int levels : {limit - 1, limit}) {
    const Value v = nested(levels);
    ASSERT_EQ(v.CheckSerializable(), Status::kSuccess) << levels;
    std::string encoded;
    ASSERT_EQ(EncodeSpillValue(v, &encoded), Status::kSuccess) << levels;
    std::string_view sv(encoded);
    auto decoded = DecodeSpillValue(&sv);
    ASSERT_TRUE(decoded.HasValue()) << decoded.GetStatus() << " at " << levels;
    EXPECT_TRUE(sv.empty()) << levels;
    EXPECT_EQ(decoded.Value(), v) << levels;
  }

  const Value too_deep = nested(limit + 1);
  EXPECT_EQ(too_deep.CheckSerializable().GetCode(), StatusCode::kTooBigData);
  std::string encoded;
  EXPECT_EQ(EncodeSpillValue(too_deep, &encoded).GetCode(),
            StatusCode::kTooBigData);
}

TEST(RowBlockCodecTest, SpillStoreRoundTripsMultipleBlocksCleanly) {
  std::error_code ec;
  const std::filesystem::path dir = std::filesystem::temp_directory_path(ec) /
                                    ("row_codec_spill_" + RandomString());
  std::filesystem::create_directories(dir, ec);
  struct Guard {
    explicit Guard(std::filesystem::path dir_path) : d(std::move(dir_path)) {}
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    Guard(Guard&&) = delete;
    Guard& operator=(Guard&&) = delete;
    ~Guard() {
      std::error_code e;
      std::filesystem::remove_all(d, e);
      if (e) {
        ADD_FAILURE() << "Failed to remove spill dir " << d << ": "
                      << e.message();
      }
    }
    std::filesystem::path d;
  } g{dir};

  SpillStore store(SpillStoreOptions{.temp_dir = dir});
  SpillSegmentWriter writer(&store, 16384);

  std::vector<Row> b1;
  b1.reserve(100);
  for (int i = 0; i < 100; ++i) {
    b1.push_back(Row({Value(int64_t{i}), Value("row_" + std::to_string(i))}));
  }
  std::vector<Row> b2;
  b2.reserve(75);
  for (int i = 100; i < 175; ++i) {
    b2.push_back(
        Row({Value(int64_t{i}), Value("second_" + std::to_string(i))}));
  }

  ASSERT_EQ(WriteRowBlock(&writer, b1), Status::kSuccess);
  ASSERT_EQ(WriteRowBlock(&writer, b2), Status::kSuccess);
  auto id_or = writer.Finish();
  ASSERT_EQ(id_or.GetStatus().GetCode(), StatusCode::kSuccess);

  SpillSegmentReader reader(&store, id_or.Value(), 8192);
  std::vector<Row> all;
  while (true) {
    std::vector<Row> block;
    auto has_more = ReadRowBlock(&reader, &block);
    ASSERT_EQ(has_more.GetStatus().GetCode(), StatusCode::kSuccess);
    if (!has_more.Value()) {
      break;
    }
    all.insert(all.end(), block.begin(), block.end());
  }
  ASSERT_EQ(all.size(), b1.size() + b2.size());
  // Verify every row, including the block boundary: checking only the two
  // ends misses reordered, duplicated or corrupted middle rows.
  for (size_t i = 0; i < b1.size(); ++i) {
    EXPECT_EQ(all[i], b1[i]) << "first block row " << i;
  }
  for (size_t i = 0; i < b2.size(); ++i) {
    EXPECT_EQ(all[b1.size() + i], b2[i]) << "second block row " << i;
  }
}

TEST(RowBlockCodecTest, RejectsCorruptTruncatedStream) {
  Row r({Value(int64_t{1}), Value("abc")});
  std::string encoded;
  ASSERT_EQ(EncodeRowBlock(std::span<const Row>(&r, 1), &encoded),
            Status::kSuccess);
  const std::string full = encoded;
  for (size_t keep = 1; keep < full.size(); ++keep) {
    encoded = full.substr(0, keep);
    std::string_view sv(encoded);
    std::vector<Row> out;
    const Status st = DecodeRowBlock(&sv, &out);
    EXPECT_EQ(st.GetCode(), StatusCode::kCorrupt) << "truncated to " << keep;
    EXPECT_TRUE(out.empty()) << "truncated to " << keep;
  }
}

TEST(RowBlockCodecTest, RejectsTrailingPayloadWithoutAppendingRows) {
  const std::array<Row, 1> input{Row({Value(7)})};
  std::string encoded;
  ASSERT_EQ(EncodeRowBlock(input, &encoded), Status::kSuccess);
  encoded.push_back('x');
  const auto payload_size = static_cast<uint32_t>(encoded.size() - 8);
  std::memcpy(encoded.data() + sizeof(uint32_t), &payload_size,
              sizeof(payload_size));
  std::string_view data(encoded);
  std::vector<Row> output{Row({Value(14)})};
  EXPECT_EQ(DecodeRowBlock(&data, &output).GetCode(), StatusCode::kCorrupt);
  EXPECT_EQ(output, (std::vector<Row>{Row({Value(14)})}));
}

TEST(RowBlockCodecTest, RejectsHugeRowCountAndOversizedPayloadWithoutOom) {
  // 1) DecodeRowBlock: row_count exceeds minimum possible rows for payload.
  {
    std::array<uint32_t, 3> raw = {0xffffffffU, 4U, 0U};
    std::string buf(reinterpret_cast<const char*>(raw.data()),
                    raw.size() * sizeof(uint32_t));
    std::string_view sv(buf);
    std::vector<Row> out;
    const Status st = DecodeRowBlock(&sv, &out);
    EXPECT_EQ(st.GetCode(), StatusCode::kCorrupt);
  }

  // 2) ReadRowBlock: payload_bytes exceeds reader->Remaining() in spill
  // segment.
  {
    std::error_code ec;
    const std::filesystem::path dir = std::filesystem::temp_directory_path(ec) /
                                      ("row_codec_corrupt_" + RandomString());
    std::filesystem::create_directories(dir, ec);
    struct Guard {
      explicit Guard(std::filesystem::path dir) : d(std::move(dir)) {}
      Guard(const Guard&) = delete;
      Guard& operator=(const Guard&) = delete;
      Guard(Guard&&) = delete;
      Guard& operator=(Guard&&) = delete;
      std::filesystem::path d;
      ~Guard() {
        std::error_code e;
        std::filesystem::remove_all(d, e);
        if (e) {
          ADD_FAILURE() << "Failed to remove spill dir " << d << ": "
                        << e.message();
        }
      }
    } g{dir};

    SpillStore store(SpillStoreOptions{.temp_dir = dir});
    SpillSegmentWriter writer(&store, 4096);
    std::array<uint32_t, 2> bogus_header = {1U, 0x7fffffffU};
    ASSERT_EQ(writer.Write(bogus_header.data(), sizeof(bogus_header)),
              Status::kSuccess);
    auto seg_or = writer.Finish();
    ASSERT_EQ(seg_or.GetStatus().GetCode(), StatusCode::kSuccess);

    SpillSegmentReader reader(&store, seg_or.Value(), 4096);
    std::vector<Row> out;
    auto res = ReadRowBlock(&reader, &out);
    EXPECT_EQ(res.GetStatus().GetCode(), StatusCode::kCorrupt);
  }
}

}  // namespace
}  // namespace tinylamb
