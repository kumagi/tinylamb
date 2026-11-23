/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/query_grant.hpp"
#include "common/random_string.hpp"
#include "common/spill/spill_store.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/ooc/chunk_codec.hpp"
#include "executor/detail/ooc/external_sorter.hpp"
#include "executor/detail/ooc/recursive_partitioner.hpp"
#include "executor/detail/ooc/spillable_store.hpp"

namespace tinylamb {
namespace {

struct TempDirGuard {
  std::filesystem::path path;
  explicit TempDirGuard(std::string_view prefix) {
    std::error_code ec;
    path = std::filesystem::temp_directory_path(ec) /
           (std::string(prefix) + "_" + RandomString());
    std::filesystem::create_directories(path, ec);
  }
  TempDirGuard(const TempDirGuard&) = delete;
  TempDirGuard& operator=(const TempDirGuard&) = delete;
  TempDirGuard(TempDirGuard&&) = delete;
  TempDirGuard& operator=(TempDirGuard&&) = delete;
  ~TempDirGuard() {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }
};

TEST(ChunkCodecTest, RoundTripsTypedNullHeteroPositionsAndLargeStrings) {
  TempDirGuard dir("ooc_chunk_codec");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  DataChunk chunk;
  const std::string big(70000, 'Q');
  chunk.Append(
      Row({Value(int64_t{-10}), Value(int64_t{99}).WithUnsigned(),
           Value::DateFromDays(19500), Value(2.5), Value(std::string(big)),
           Value("Alpha").WithCollation(2), Value("1 day").WithInterval(),
           Value::Array({Value(int64_t{1}), Value(int64_t{2})}, "INT64")}),
      RowPosition(11, 3));
  chunk.Append(Row({Value(), Value(), Value(), Value(), Value(),
                    Value("beta").WithCollation(2),
                    Value("2 days").WithInterval(), Value()}),
               RowPosition(12, 7));
  chunk.ColumnAt(0).SetAllowHeterogeneous(true);
  // Trigger heterogeneous column on column 0 by mixing ValueType::kVarChar.
  chunk.Append(Row({Value("mixed"), Value(int64_t{42}).WithUnsigned(),
                    Value::DateFromDays(19600), Value(-0.0), Value("small"),
                    Value("GAMMA").WithCollation(2), Value(),
                    Value::Array({Value()}, "INT64")}),
               RowPosition(13, 9));

  std::string encoded;
  ASSERT_EQ(EncodeDataChunk(chunk, &encoded), Status::kSuccess);
  std::string_view sv(encoded);
  DataChunk decoded;
  ASSERT_EQ(DecodeDataChunk(&sv, &decoded), Status::kSuccess);
  EXPECT_TRUE(sv.empty());
  ASSERT_EQ(decoded.Size(), 3U);
  for (size_t r = 0; r < 3; ++r) {
    EXPECT_EQ(decoded.RowAt(r), chunk.RowAt(r)) << "row=" << r;
    EXPECT_EQ(decoded.PositionAt(r), chunk.PositionAt(r)) << "row=" << r;
  }
  EXPECT_TRUE(decoded.ColumnAt(1).IsUnsigned());
  EXPECT_EQ(decoded.ColumnAt(5).ColumnCollation(), Collation::kCaseInsensitive);
  EXPECT_TRUE(decoded.RowAt(0)[6].IsInterval());

  // Also test SpillStore chunk streaming.
  SpillSegmentWriter writer(&store, 16384);
  ASSERT_EQ(WriteDataChunk(&writer, chunk), Status::kSuccess);
  auto seg = writer.Finish();
  ASSERT_EQ(seg.GetStatus().GetCode(), StatusCode::kSuccess);
  SpillSegmentReader reader(&store, seg.Value(), 8192);
  DataChunk streamed;
  auto has_chunk = ReadDataChunk(&reader, &streamed);
  ASSERT_EQ(has_chunk.GetStatus().GetCode(), StatusCode::kSuccess);
  EXPECT_TRUE(has_chunk.Value());
  EXPECT_EQ(streamed.Size(), 3U);
  EXPECT_EQ(streamed.RowAt(0), chunk.RowAt(0));
  has_chunk = ReadDataChunk(&reader, &streamed);
  ASSERT_EQ(has_chunk.GetStatus().GetCode(), StatusCode::kSuccess);
  EXPECT_FALSE(has_chunk.Value());
}

TEST(ChunkCodecTest, HeteroColumnRejectsOutOfRangeCollationBits) {
  // The collation field is present in every column header, including
  // heterogeneous frames whose values carry per-value tags; an out-of-range
  // value (3) must be rejected as corrupt rather than silently ignored.
  DataChunk chunk;
  chunk.Append(Row({Value(int64_t{1})}), RowPosition());
  chunk.Append(Row({Value(int64_t{2})}), RowPosition());
  chunk.ColumnAt(0).SetAllowHeterogeneous(true);
  chunk.Append(Row({Value("mixed")}), RowPosition());

  std::string encoded;
  ASSERT_EQ(EncodeDataChunk(chunk, &encoded), Status::kSuccess);
  ASSERT_GT(encoded.size(), 10U);
  // Header: u32 rows + u32 cols + u8 has_positions = 9 bytes; column 0 then
  // stores raw_type at [9] and flags at [10].
  encoded[10] = static_cast<char>(encoded[10] | 0x60);  // collation bits = 3

  std::string_view sv(encoded);
  DataChunk decoded;
  const Status st = DecodeDataChunk(&sv, &decoded);
  EXPECT_EQ(st.GetCode(), StatusCode::kCorrupt);
}

TEST(ChunkCodecTest, NullOnlySliceRetainsColumnCollation) {
  DataChunk source;
  source.Append(Row({Value("Alpha").WithCollation(
      static_cast<uint8_t>(Collation::kCaseInsensitive))}));
  source.Append(Row({Value()}));

  DataChunk fragment;
  fragment.AppendFrom(source, 1);
  ASSERT_EQ(fragment.ColumnAt(0).ColumnCollation(),
            Collation::kCaseInsensitive);

  std::string encoded;
  ASSERT_EQ(EncodeDataChunk(fragment, &encoded), Status::kSuccess);
  std::string_view payload(encoded);
  DataChunk decoded;
  ASSERT_EQ(DecodeDataChunk(&payload, &decoded), Status::kSuccess);
  EXPECT_TRUE(payload.empty());
  ASSERT_EQ(decoded.Size(), 1U);
  EXPECT_EQ(decoded.ColumnAt(0).Type(), ValueType::kVarChar);
  EXPECT_TRUE(decoded.ColumnAt(0).IsNull(0));
  EXPECT_EQ(decoded.ColumnAt(0).ColumnCollation(), Collation::kCaseInsensitive);
}

TEST(ChunkCodecTest, DecodeFailureClearsPartialColumnsAndPreservesInput) {
  DataChunk chunk;
  chunk.Append(Row({Value(7), Value("truncated string")}));
  std::string encoded;
  ASSERT_EQ(EncodeDataChunk(chunk, &encoded), Status::kSuccess);
  encoded.pop_back();
  std::string_view payload(encoded);
  DataChunk output;
  output.Append(Row({Value(99)}));

  EXPECT_EQ(DecodeDataChunk(&payload, &output), StatusCode::kCorrupt);
  EXPECT_EQ(payload, std::string_view(encoded));
  EXPECT_EQ(output.Size(), 0U);
  EXPECT_EQ(output.ColumnCount(), 0U);
}

TEST(ChunkCodecTest, EncodeAppendsAndDecodeConsumesExactlyOneChunk) {
  DataChunk first;
  first.Append(Row({Value(1)}));
  DataChunk second;
  second.Append(Row({Value("two")}));
  const std::string prefix = "prefix";
  std::string encoded = prefix;
  ASSERT_EQ(EncodeDataChunk(first, &encoded), Status::kSuccess);
  ASSERT_EQ(EncodeDataChunk(second, &encoded), Status::kSuccess);
  ASSERT_TRUE(encoded.starts_with(prefix));

  std::string_view payload(encoded);
  payload.remove_prefix(prefix.size());
  DataChunk output;
  ASSERT_EQ(DecodeDataChunk(&payload, &output), Status::kSuccess);
  EXPECT_EQ(output.RowAt(0), first.RowAt(0));
  ASSERT_EQ(DecodeDataChunk(&payload, &output), Status::kSuccess);
  EXPECT_EQ(output.RowAt(0), second.RowAt(0));
  EXPECT_TRUE(payload.empty());
}

TEST(ChunkCodecTest, RejectsShortVarcharColumnBeforeEstimatingFrameSize) {
  DataChunk chunk;
  chunk.Append(Row({Value(1), Value("first")}));
  chunk.Append(Row({Value(2), Value("second")}));
  chunk.ColumnAt(1).Reset();

  std::string encoded = "prefix";
  EXPECT_EQ(EncodeDataChunk(chunk, &encoded), StatusCode::kInvalidArgument);
  EXPECT_EQ(encoded, "prefix");

  TempDirGuard dir("ooc_chunk_short_column");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  SpillSegmentWriter writer(&store, 4096);
  EXPECT_EQ(WriteDataChunk(&writer, chunk, nullptr, 4096),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(writer.BytesWritten(), 0U);
}

TEST(ChunkCodecTest, NullColumnsRoundTripWithoutPositions) {
  DataChunk chunk;
  chunk.Initialize({ValueType::kNull, ValueType::kNull}, 0);
  const Row row({Value(), Value()});
  for (int i = 0; i < 128; ++i) {
    chunk.Append(row);
  }
  std::string encoded;
  ASSERT_EQ(EncodeDataChunk(chunk, &encoded), Status::kSuccess);
  std::string_view payload(encoded);
  DataChunk decoded;
  ASSERT_EQ(DecodeDataChunk(&payload, &decoded), Status::kSuccess);
  EXPECT_TRUE(payload.empty());
  ASSERT_EQ(decoded.Size(), chunk.Size());
  for (size_t r = 0; r < decoded.Size(); ++r) {
    EXPECT_EQ(decoded.RowAt(r), row);
    EXPECT_FALSE(decoded.PositionAt(r).IsValid());
  }
}

TEST(ChunkCodecTest, WriteSplitsOversizedChunkIntoBoundedFrames) {
  TempDirGuard dir("ooc_chunk_split");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  DataChunk chunk;
  const std::string payload(1000, 'x');
  for (int64_t i = 0; i < 200; ++i) {
    chunk.Append(
        Row({Value(i),
             Value(i % 5 == 0 ? std::string() : payload + std::to_string(i))
                 .WithCollation(i % 3 == 0 ? 0 : 2),
             Value(static_cast<double>(i))}),
        RowPosition(4, static_cast<slot_t>(i)));
  }
  ASSERT_EQ(chunk.Size(), 200U);

  constexpr size_t kCap = 32U << 10;
  ASSERT_GT(EstimateChunkMemoryBytes(chunk), kCap);
  SpillSegmentWriter writer(&store, 16384);
  ASSERT_EQ(WriteDataChunk(&writer, chunk, nullptr, kCap), Status::kSuccess);
  auto seg = writer.Finish();
  ASSERT_EQ(seg.GetStatus().GetCode(), StatusCode::kSuccess);

  // Every emitted frame must respect the cap and hold at least one row.
  size_t frames = 0;
  {
    SpillSegmentReader reader(&store, seg.Value(), 8192);
    ASSERT_EQ(reader.Open().GetCode(), StatusCode::kSuccess);
    std::vector<char> skip;
    while (!reader.Eof()) {
      uint32_t len = 0;
      ASSERT_EQ(reader.ReadPod(&len).GetCode(), StatusCode::kSuccess);
      EXPECT_LE(len, kCap) << "frame=" << frames;
      skip.resize(len);
      if (len > 0) {
        ASSERT_EQ(reader.ReadExact(skip.data(), len).GetCode(),
                  StatusCode::kSuccess);
      }
      ++frames;
    }
  }
  EXPECT_GE(frames, 4U);
  EXPECT_LT(frames, 200U);

  // Row-for-row reassembly: same order, values, collation, and positions.
  SpillSegmentReader reader(&store, seg.Value(), 8192);
  size_t seen = 0;
  DataChunk streamed;
  while (true) {
    auto has = ReadDataChunk(&reader, &streamed);
    ASSERT_EQ(has.GetStatus().GetCode(), StatusCode::kSuccess);
    if (!has.Value()) {
      break;
    }
    for (size_t r = 0; r < streamed.Size(); ++r, ++seen) {
      ASSERT_LT(seen, 200U);
      EXPECT_EQ(streamed.RowAt(r), chunk.RowAt(seen)) << "row=" << seen;
      EXPECT_EQ(streamed.PositionAt(r), chunk.PositionAt(seen))
          << "row=" << seen;
    }
  }
  EXPECT_EQ(seen, 200U);
}

TEST(ChunkCodecTest, AppliesTheSameRowLimitWhenEncodingAndDecoding) {
  constexpr size_t kFrameRows = 1U << 22;
  DataChunk chunk;
  chunk.CommitBatchAppend(kFrameRows + 1);
  std::string encoded = "prefix";
  EXPECT_EQ(EncodeDataChunk(chunk, &encoded), StatusCode::kTooBigData);
  EXPECT_EQ(encoded, "prefix");

  TempDirGuard dir("ooc_chunk_row_limit");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  SpillSegmentWriter writer(&store, 4096);
  ASSERT_EQ(WriteDataChunk(&writer, chunk), Status::kSuccess);
  const auto segment = writer.Finish();
  ASSERT_TRUE(segment.HasValue());

  SpillSegmentReader reader(&store, segment.Value(), 4096);
  DataChunk decoded;
  auto has_frame = ReadDataChunk(&reader, &decoded);
  ASSERT_TRUE(has_frame.HasValue() && has_frame.Value());
  EXPECT_EQ(decoded.Size(), kFrameRows);
  has_frame = ReadDataChunk(&reader, &decoded);
  ASSERT_TRUE(has_frame.HasValue() && has_frame.Value());
  EXPECT_EQ(decoded.Size(), 1U);
  has_frame = ReadDataChunk(&reader, &decoded);
  ASSERT_TRUE(has_frame.HasValue());
  EXPECT_FALSE(has_frame.Value());
}

TEST(SpillableStoreTest, RowLargerThanBudgetSpillsWithoutFullFrameDup) {
  TempDirGuard dir("ooc_spillable_bigrow");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  SpillableStore sstore(SpillableStore::Options{
      .spill_store = &store, .memory_budget_bytes = 64U << 10});

  const std::string big(200U << 10, 'Z');
  for (int64_t i = 0; i < 8; ++i) {
    ASSERT_EQ(sstore.AppendRow(Row({Value(i), Value(std::string(big))}),
                               RowPosition(2, static_cast<slot_t>(i))),
              Status::kSuccess);
  }
  ASSERT_EQ(sstore.Seal(), Status::kSuccess);
  EXPECT_TRUE(sstore.Spilled());
  ASSERT_EQ(sstore.ResetCursor(), Status::kSuccess);
  for (int64_t i = 0; i < 8; ++i) {
    Row row;
    RowPosition pos;
    auto has = sstore.NextRow(&row, &pos);
    ASSERT_EQ(has.GetStatus().GetCode(), StatusCode::kSuccess);
    ASSERT_TRUE(has.Value());
    EXPECT_EQ(row, (Row{Value(i), Value(std::string(big))})) << "row=" << i;
    EXPECT_EQ(pos, (RowPosition(2, static_cast<slot_t>(i))));
  }
  Row row;
  auto has = sstore.NextRow(&row);
  ASSERT_EQ(has.GetStatus().GetCode(), StatusCode::kSuccess);
  EXPECT_FALSE(has.Value());
}

TEST(SpillableStoreTest, InMemoryFastPathAvoidsDiskIo) {
  TempDirGuard dir("ooc_spillable_fast");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  SpillableStore sstore(SpillableStore::Options{
      .spill_store = &store, .memory_budget_bytes = 2ULL << 20});

  for (int i = 0; i < 500; ++i) {
    ASSERT_EQ(sstore.AppendRow(
                  Row({Value(int64_t{i}), Value("val_" + std::to_string(i))}),
                  RowPosition(1, static_cast<slot_t>(i))),
              Status::kSuccess);
  }
  ASSERT_EQ(sstore.Seal(), Status::kSuccess);
  EXPECT_FALSE(sstore.Spilled());
  EXPECT_EQ(store.Stats().open_fds, 0);
  EXPECT_EQ(sstore.RowCount(), 500U);

  for (int pass = 0; pass < 2; ++pass) {
    ASSERT_EQ(sstore.ResetCursor(), Status::kSuccess);
    int64_t idx = 0;
    Row row;
    RowPosition pos;
    while (true) {
      auto has = sstore.NextRow(&row, &pos);
      ASSERT_EQ(has.GetStatus().GetCode(), StatusCode::kSuccess);
      if (!has.Value()) {
        break;
      }
      EXPECT_EQ(row[0].AsInt64(), idx);
      EXPECT_EQ(pos, RowPosition(1, static_cast<slot_t>(idx)));
      ++idx;
    }
    EXPECT_EQ(idx, 500);
  }
}

TEST(SpillableStoreTest, FailedTransitionPreservesBufferedChunks) {
  TempDirGuard dir("ooc_spillable_failed_transition");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  SpillableStore sstore(SpillableStore::Options{.spill_store = &store,
                                                .memory_budget_bytes = 4096,
                                                .io_buffer_bytes = 0});
  for (int value : {1, 2}) {
    DataChunk chunk;
    chunk.Append(Row({Value(value)}), RowPosition());
    ASSERT_EQ(sstore.AppendChunk(std::move(chunk)), Status::kSuccess);
  }
  DataChunk large;
  large.Append(Row({Value(std::string(8192, 'x'))}), RowPosition());
  auto& mem = ProcessMemoryBudget::Global();
  mem.SetTestSpillIoFailCountdown(2);
  const Status failure = sstore.AppendChunk(std::move(large));
  mem.SetTestSpillIoFailCountdown(0);
  EXPECT_EQ(failure.GetCode(), StatusCode::kIOError);
  ASSERT_FALSE(sstore.Spilled());
  EXPECT_EQ(store.Stats().live_segments, 0U);
  ASSERT_EQ(sstore.AppendChunk(std::move(large)), Status::kSuccess);
  ASSERT_TRUE(sstore.Spilled());
  ASSERT_EQ(sstore.Seal(), Status::kSuccess);
  Row row;
  for (int value : {1, 2}) {
    const auto found = sstore.NextRow(&row);
    ASSERT_TRUE(found.HasValue());
    ASSERT_TRUE(found.Value());
    EXPECT_EQ(row, Row({Value(value)}));
  }
  const auto retried = sstore.NextRow(&row);
  ASSERT_TRUE(retried.HasValue());
  ASSERT_TRUE(retried.Value());
  EXPECT_EQ(row, Row({Value(std::string(8192, 'x'))}));
  const auto ended = sstore.NextRow(&row);
  ASSERT_TRUE(ended.HasValue());
  EXPECT_FALSE(ended.Value());
}

TEST(SpillableStoreTest, SpillsOverBudgetAndCleansUpOnReset) {
  TempDirGuard dir("ooc_spillable_spill");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  struct ResetGuard {
    explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
    ResetGuard(const ResetGuard&) = delete;
    ResetGuard& operator=(const ResetGuard&) = delete;
    ResetGuard(ResetGuard&&) = delete;
    ResetGuard& operator=(ResetGuard&&) = delete;
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } guard(&mb);
  constexpr size_t kLimit = 64ULL << 20;
  mb.ResetForTest(kLimit, kLimit, kLimit, false);
  QueryMemoryCoordinator coord(&mb);
  auto grant_or = coord.AcquireGrant(512ULL << 10, 512ULL << 10);
  ASSERT_TRUE(grant_or.HasValue()) << grant_or.GetStatus();
  auto grant = grant_or.MoveValue();

  {
    SpillableStore sstore(SpillableStore::Options{
        .spill_store = &store,
        .memory_budget_bytes = 256ULL << 10,
        .grant = &grant,
    });
    const std::string payload(512, 'P');
    for (int i = 0; i < 2500; ++i) {
      ASSERT_EQ(sstore.AppendRow(
                    Row({Value(int64_t{i}), Value(std::string(payload))})),
                Status::kSuccess);
    }
    ASSERT_EQ(sstore.Seal(), Status::kSuccess);
    EXPECT_TRUE(sstore.Spilled());
    EXPECT_EQ(sstore.InMemoryBytes(), 0U);
    EXPECT_GT(sstore.SpilledBytes(), 0U);
    EXPECT_EQ(grant.ActiveConsumerCount(), 1U);

    // Multi-pass cursor over spilled stream.
    for (int pass = 0; pass < 2; ++pass) {
      ASSERT_EQ(sstore.ResetCursor(), Status::kSuccess);
      int64_t count = 0;
      Row r;
      while (true) {
        auto has = sstore.NextRow(&r);
        ASSERT_EQ(has.GetStatus().GetCode(), StatusCode::kSuccess);
        if (!has.Value()) {
          break;
        }
        EXPECT_EQ(r[0].AsInt64(), count);
        ++count;
      }
      EXPECT_EQ(count, 2500);
    }
  }

  EXPECT_EQ(store.Stats().live_segments, 0U);
  EXPECT_EQ(store.Stats().current_active_bytes, 0U);
  EXPECT_EQ(grant.ActiveConsumerCount(), 0U);
  EXPECT_EQ(grant.UsedBytes(), 0U);
}

TEST(OocPrimitivesSorterTest, InMemoryFastPathAndTopNBoundedHeap) {
  TempDirGuard dir("ooc_sorter_mem");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  // Check NULLS FIRST/LAST, DESC, and CaseInsensitive collation.
  ExternalSorter sorter(ExternalSorter::Options{
      .spill_store = &store,
      .memory_budget_bytes = 4ULL << 20,
      .keys =
          {
              OocSortKey{.column_index = 0,
                         .ascending = true,
                         .nulls_first = false,
                         .collation = Collation::kCaseInsensitive},
              OocSortKey{
                  .column_index = 1, .ascending = false, .nulls_first = true},
          },
      .limit = std::nullopt,
  });

  ASSERT_EQ(sorter.AddRow(Row({Value("beta"), Value(int64_t{1})})),
            Status::kSuccess);
  ASSERT_EQ(sorter.AddRow(Row({Value("Alpha"), Value(int64_t{2})})),
            Status::kSuccess);
  ASSERT_EQ(sorter.AddRow(Row({Value("alpha"), Value()})), Status::kSuccess);
  ASSERT_EQ(sorter.AddRow(Row({Value(), Value(int64_t{99})})),
            Status::kSuccess);
  ASSERT_EQ(sorter.Finish(), Status::kSuccess);
  EXPECT_FALSE(sorter.Spilled());
  EXPECT_EQ(store.Stats().open_fds, 0);

  std::vector<Row> out;
  Row r;
  StatusOr<bool> next = sorter.NextRow(&r);
  while (next.HasValue() && next.Value()) {
    out.push_back(r);
    next = sorter.NextRow(&r);
  }
  ASSERT_EQ(next.GetStatus(), Status::kSuccess);
  ASSERT_EQ(out.size(), 4U);
  // NULLS FIRST for col 1 puts ("alpha", NULL) before ("Alpha", 2).
  EXPECT_TRUE(out[0][1].IsNull());
  EXPECT_EQ(out[1][1].AsInt64(), 2);
  EXPECT_EQ(out[2][0].value.varchar_value, "beta");
  EXPECT_TRUE(out[3][0].IsNull());

  // Bounded Top-N heap keeps memory << total input without spilling.
  ExternalSorter topn(ExternalSorter::Options{
      .spill_store = &store,
      .memory_budget_bytes = 256ULL << 10,
      .keys = {OocSortKey{.column_index = 0, .ascending = true}},
      .limit = 15,
  });
  const std::string filler(256, 'T');
  for (int i = 10000; i >= 1; --i) {
    ASSERT_EQ(topn.AddRow(Row({Value(int64_t{i}), Value(std::string(filler))})),
              Status::kSuccess);
  }
  ASSERT_EQ(topn.Finish(), Status::kSuccess);
  EXPECT_FALSE(topn.Spilled());
  EXPECT_EQ(store.Stats().open_fds, 0);
  int64_t expected = 1;
  StatusOr<bool> topn_has = topn.NextRow(&r);
  while (topn_has.HasValue() && topn_has.Value()) {
    EXPECT_EQ(r[0].AsInt64(), expected++);
    topn_has = topn.NextRow(&r);
  }
  ASSERT_EQ(topn_has.GetStatus(), Status::kSuccess);
  EXPECT_EQ(expected, 16);
}

TEST(OocPrimitivesSorterTest, MultiPassExternalMergeUnderTightBudget) {
  TempDirGuard dir("ooc_sorter_ext");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  // 20,000 rows x ~350B ~= 7 MiB under 256 KiB budget -> ~30+ runs, >= 2 merge
  // passes with bounded fan-in.
  constexpr int kRows = 20000;
  ExternalSorter sorter(ExternalSorter::Options{
      .spill_store = &store,
      .memory_budget_bytes = 256ULL << 10,
      .keys = {OocSortKey{.column_index = 0, .ascending = true}},
      .limit = std::nullopt,
  });

  const std::string pad(256, 'S');
  for (int i = 0; i < kRows; ++i) {
    // Deterministic permutation mod kRows (gcd(7919, 20000) == 1).
    const int64_t key = (static_cast<int64_t>(i) * 7919LL) % kRows;
    ASSERT_EQ(sorter.AddRow(Row({Value(key), Value(std::string(pad))})),
              Status::kSuccess);
  }
  ASSERT_EQ(sorter.Finish(), Status::kSuccess);
  EXPECT_TRUE(sorter.Spilled());
  EXPECT_GE(sorter.RunCount(), 20U);
  EXPECT_GE(sorter.MergePasses(), 2U);
  EXPECT_LE(sorter.PeakMemoryBytes(), 280ULL << 10);

  DataChunk chunk;
  int64_t next_expected = 0;
  while (true) {
    auto has = sorter.NextChunk(&chunk, 512);
    ASSERT_EQ(has.GetStatus().GetCode(), StatusCode::kSuccess);
    if (!has.Value()) {
      break;
    }
    for (size_t r = 0; r < chunk.Size(); ++r) {
      EXPECT_EQ(chunk.RowAt(r)[0].AsInt64(), next_expected++);
    }
  }
  EXPECT_EQ(next_expected, kRows);
  EXPECT_EQ(store.Stats().live_segments, 0U);
  EXPECT_EQ(store.Stats().current_active_bytes, 0U);
}

// P34/61: the K-way merge compares per-stream cached key cells in a loser
// tree with an explicit stream-index tie-break.  Runs are written in
// insertion order and stable-sorted, so a fully-tied input must merge out in
// original insertion order (global sort stability across runs).  The old
// binary heap over Rows broke ties by heap sifting, not by run order.
TEST(OocPrimitivesSorterTest, FinishRetriesAfterAnIntermediateGroupWasMerged) {
  TempDirGuard dir("ooc_sort_retry");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  bool retried_partial_pass = false;
  for (unsigned fault = 1; fault < 64 && !retried_partial_pass; ++fault) {
    ExternalSorter sorter(ExternalSorter::Options{
        .spill_store = &store, .memory_budget_bytes = 1,
        .keys = {OocSortKey{.column_index = 0}}, .limit = 8});
    for (int key = 9; key > 0; --key) {
      ASSERT_EQ(sorter.AddRow(Row({Value(key)})), Status::kSuccess);
    }
    const auto before = sorter.SpilledBytes();
    auto& budget = ProcessMemoryBudget::Global();
    budget.SetTestSpillIoFailCountdown(fault);
    const auto failed = sorter.Finish();
    budget.SetTestSpillIoFailCountdown(0);
    if (failed == Status::kSuccess || before == sorter.SpilledBytes()) {
      continue;
    }
    ASSERT_EQ(failed, Status::kIOError);
    ASSERT_EQ(sorter.Finish(), Status::kSuccess);
    Row row;
    for (int expected = 1; expected <= 8; ++expected) {
      auto next = sorter.NextRow(&row);
      ASSERT_TRUE(next.HasValue() && next.Value());
      EXPECT_EQ(row, Row({Value(expected)}));
    }
    for (int i = 0; i < 2; ++i) {
      const auto end = sorter.NextRow(&row);
      ASSERT_TRUE(end.HasValue());
      EXPECT_FALSE(end.Value());
      EXPECT_EQ(store.Stats().live_segments, 0U);
    }
    retried_partial_pass = true;
  }
  EXPECT_TRUE(retried_partial_pass);
}

TEST(OocPrimitivesSorterTest, WideRowsRoundTripUnderSmallBudget) {
  TempDirGuard dir("ooc_sorter_wide");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  ExternalSorter sorter(ExternalSorter::Options{
      .spill_store = &store,
      .memory_budget_bytes = 8192,
      .keys = {OocSortKey{.column_index = 0, .ascending = true}},
      .limit = std::nullopt});
  const Row large({Value(int64_t{2}), Value(std::string(16000, 'b'))});
  const Row small({Value(int64_t{1}), Value(std::string(3000, 'a'))});
  ASSERT_EQ(sorter.AddRow(large), Status::kSuccess);
  ASSERT_EQ(sorter.AddRow(small), Status::kSuccess);
  ASSERT_EQ(sorter.Finish(), Status::kSuccess);
  EXPECT_TRUE(sorter.Spilled());
  Row row;
  auto first = sorter.NextRow(&row);
  ASSERT_EQ(first.GetStatus(), Status::kSuccess);
  ASSERT_TRUE(first.Value());
  EXPECT_EQ(row, small);
  auto second = sorter.NextRow(&row);
  ASSERT_EQ(second.GetStatus(), Status::kSuccess);
  ASSERT_TRUE(second.Value());
  EXPECT_EQ(row, large);
  auto end = sorter.NextRow(&row);
  ASSERT_EQ(end.GetStatus(), Status::kSuccess);
  EXPECT_FALSE(end.Value());
}

TEST(OocPrimitivesSorterTest, CrossRunTieOrderIsStreamStable) {
  TempDirGuard dir("ooc_sorter_ties");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  constexpr int kRows = 4000;
  ExternalSorter sorter(ExternalSorter::Options{
      .spill_store = &store,
      .memory_budget_bytes = 32ULL << 10,
      .keys = {OocSortKey{.column_index = 0, .ascending = true}},
      .limit = std::nullopt,
      .force_spill = true,
  });
  const std::string pad(160, 'P');
  for (int i = 0; i < kRows; ++i) {
    ASSERT_EQ(
        sorter.AddRow(Row({Value(int64_t{7}), Value(std::to_string(i) + pad)})),
        Status::kSuccess);
  }
  ASSERT_EQ(sorter.Finish(), Status::kSuccess);
  EXPECT_TRUE(sorter.Spilled());
  EXPECT_GE(sorter.RunCount(), 4U);
  Row r;
  int64_t next_payload = 0;
  StatusOr<bool> has = sorter.NextRow(&r);
  while (has.HasValue() && has.Value()) {
    const std::string_view payload = r[1].AsStringView();
    const int64_t idx = std::strtoll(std::string(payload).c_str(), nullptr, 10);
    EXPECT_EQ(idx, next_payload++)
        << "row " << next_payload - 1 << " lost run-order stability";
    has = sorter.NextRow(&r);
  }
  ASSERT_EQ(has.GetStatus(), Status::kSuccess);
  EXPECT_EQ(next_payload, kRows);
  EXPECT_EQ(store.Stats().live_segments, 0U);
}

TEST(OocPrimitivesSorterTest, FaultInjectionAndTempLimitCleanUpAllSegments) {
  TempDirGuard dir("ooc_sorter_fault");
  ProcessMemoryBudget& mem = ProcessMemoryBudget::Global();
  struct Cleanup {
    explicit Cleanup(ProcessMemoryBudget* budget) : m(budget) {}
    Cleanup(const Cleanup&) = delete;
    Cleanup& operator=(const Cleanup&) = delete;
    Cleanup(Cleanup&&) = delete;
    Cleanup& operator=(Cleanup&&) = delete;
    ~Cleanup() { m->SetTestSpillIoFailCountdown(0); }
    ProcessMemoryBudget* m;
  } cleanup(&mem);

  {
    SpillStore tiny_store(SpillStoreOptions{
        .temp_dir = dir.path,
        .temp_limit_bytes = 2ULL * SpillStore::kBlockSize,
        .use_global_temp_limit = false,
    });
    {
      ExternalSorter sorter(ExternalSorter::Options{
          .spill_store = &tiny_store,
          .memory_budget_bytes = 64ULL << 10,
          .keys = {OocSortKey{.column_index = 0}},
          .limit = std::nullopt,
      });
      const std::string pad(512, 'X');
      Status st = Status::kSuccess;
      for (int i = 0; i < 2000; ++i) {
        st = sorter.AddRow(Row({Value(int64_t{i}), Value(std::string(pad))}));
        if (st != Status::kSuccess) {
          break;
        }
      }
      EXPECT_EQ(st.GetCode(), StatusCode::kNoSpace);
    }
    EXPECT_EQ(tiny_store.Stats().live_segments, 0U);
    EXPECT_EQ(tiny_store.Stats().current_active_bytes, 0U);
  }

  {
    SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
    {
      ExternalSorter sorter(ExternalSorter::Options{
          .spill_store = &store,
          .memory_budget_bytes = 64ULL << 10,
          .keys = {OocSortKey{.column_index = 0}},
          .limit = std::nullopt,
      });
      const std::string pad(256, 'Y');
      for (int i = 0; i < 1500; ++i) {
        ASSERT_EQ(sorter.AddRow(
                      Row({Value(int64_t{1500 - i}), Value(std::string(pad))})),
                  Status::kSuccess);
      }
      mem.SetTestSpillIoFailCountdown(1);
      const Status st = sorter.Finish();
      EXPECT_EQ(st.GetCode(), StatusCode::kIOError);
    }
    EXPECT_EQ(store.Stats().live_segments, 0U);
    EXPECT_EQ(store.Stats().current_active_bytes, 0U);
  }
}

TEST(RecursivePartitionerTest, WriterBuffersFitSmallOperatorBudgets) {
  for (size_t budget : {size_t{1}, size_t{8192}, size_t{16384}, size_t{32768},
                        size_t{524288}}) {
    size_t writer_bytes = 0;
    const uint32_t bits =
        RecursivePartitioner::ComputeFanoutBits(budget, &writer_bytes);
    EXPECT_LE((size_t{1} << bits) * writer_bytes, budget / 2) << budget;
  }
}

TEST(RecursivePartitionerTest, RejectsUseAfterSpillStoreDestruction) {
  auto store = std::make_unique<SpillStore>();
  auto partitioner = std::make_unique<RecursivePartitioner>(
      RecursivePartitioner::Options{.spill_store = store.get(),
                                    .memory_budget_bytes = 65536,
                                    .force_spill = true},
      [](const Row&) { return uint64_t{0}; });
  ASSERT_EQ(partitioner->AddRow(Row({Value(1)})), Status::kSuccess);
  store.reset();
  PartitionLeafSet result;
  EXPECT_NE(partitioner->Finalize(&result), Status::kSuccess);
  EXPECT_NE(partitioner->AddRow(Row({Value(2)})), Status::kSuccess);
  partitioner.reset();  // writer cleanup must not access the dead store
}

TEST(RecursivePartitionerTest, ThrowingHashFunctionSurfacesAsStatus) {
  auto throwing_hash = [](const Row&) -> uint64_t {
    throw std::runtime_error("hash boom");
  };
  TempDirGuard dir("ooc_part_throw");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  // Spilled AddRow path: hashing runs after TransitionToSpill.
  {
    RecursivePartitioner part(
        RecursivePartitioner::Options{.spill_store = &store,
                                      .memory_budget_bytes = 64ULL << 10,
                                      .force_spill = true},
        throwing_hash);
    EXPECT_EQ(part.AddRow(Row({Value(1)})).GetCode(),
              StatusCode::kRuntimeError);
  }
  // TransitionToSpill path: rows buffered in memory are hashed during the
  // spill transition; a throwing hash_fn_ must surface as a Status, not an
  // exception crossing the Status boundary.
  {
    RecursivePartitioner part(
        RecursivePartitioner::Options{.spill_store = &store,
                                      .memory_budget_bytes = 4096},
        throwing_hash);
    ASSERT_EQ(part.AddRow(Row({Value(1)})), Status::kSuccess);
    EXPECT_EQ(part.AddRow(Row({Value(std::string(8192, 'x'))})).GetCode(),
              StatusCode::kRuntimeError);
  }
}

TEST(RecursivePartitionerTest, UniformKeysReachDepth3AndReconstructAllRows) {
  TempDirGuard dir("ooc_part_depth3");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  // Budget = 64 KiB -> F = 4 (2 bits/level).
  // ~18,000 rows x ~300B ~= 5.4 MiB -> splits to depth 3 (~21 KiB <= 64 KiB)
  // without triggering skew!
  constexpr int kRows = 18000;
  constexpr size_t kBudget = 64ULL << 10;

  auto hash_fn = [](const Row& r) -> uint64_t {
    uint64_t x = static_cast<uint64_t>(r[0].AsInt64()) + 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
  };

  PartitionLeafSet leaves;
  uint32_t max_depth = 0;
  uint32_t skews = 0;
  {
    RecursivePartitioner part(
        RecursivePartitioner::Options{
            .spill_store = &store,
            .memory_budget_bytes = kBudget,
            .max_depth = 8,
        },
        hash_fn);
    const std::string pad(220, 'U');
    for (int i = 0; i < kRows; ++i) {
      ASSERT_EQ(part.AddRow(Row({Value(int64_t{i}), Value(std::string(pad))})),
                Status::kSuccess);
    }
    ASSERT_EQ(part.Finalize(&leaves), Status::kSuccess);
    EXPECT_TRUE(part.Spilled());
    max_depth = part.MaxObservedDepth();
    skews = part.SkewFallbackCount();
  }

  EXPECT_GE(max_depth, 3U);
  EXPECT_EQ(skews, 0U);

  std::vector<bool> seen(kRows, false);
  uint64_t recovered = 0;
  for (size_t li = 0; li < leaves.leaves.size(); ++li) {
    const PartitionLeaf& leaf = leaves.leaves[li];
    EXPECT_FALSE(leaf.skew_fallback);
    EXPECT_LE(leaf.estimated_bytes, kBudget);
    ASSERT_EQ(
        RecursivePartitioner::ReadLeafRows(
            &store, leaf,
            [&](Row&& r) {
              const int64_t id = r[0].AsInt64();
              if (id >= 0 && id < kRows && !seen[static_cast<size_t>(id)]) {
                seen[static_cast<size_t>(id)] = true;
                ++recovered;
              }
              return Status::kSuccess;
            }),
        Status::kSuccess);
    leaves.DropLeaf(li);
  }
  EXPECT_EQ(recovered, static_cast<uint64_t>(kRows));
  EXPECT_EQ(store.Stats().live_segments, 0U);
  EXPECT_EQ(store.Stats().current_active_bytes, 0U);
}

TEST(RecursivePartitionerTest,
     SkewedHeavyHitterDetectsLt20PercentShrinkFallback) {
  TempDirGuard dir("ooc_part_skew");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  constexpr int kRows = 2500;
  constexpr size_t kBudget = 64ULL << 10;
  auto hash_fn = [](const Row& r) -> uint64_t {
    uint64_t x = static_cast<uint64_t>(r[0].AsInt64()) * 0x9e3779b97f4a7c15ULL;
    return (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  };

  PartitionLeafSet leaves;
  RecursivePartitioner part(
      RecursivePartitioner::Options{
          .spill_store = &store,
          .memory_budget_bytes = kBudget,
          .max_depth = 8,
      },
      hash_fn);

  const std::string pad(200, 'K');
  // 92% duplicate key = 42 (< 20% shrink -> immediate skew fallback).
  for (int i = 0; i < kRows; ++i) {
    const int64_t k = (i % 12 == 0) ? i : 42;
    ASSERT_EQ(part.AddRow(
                  Row({Value(k), Value(int64_t{i}), Value(std::string(pad))})),
              Status::kSuccess);
  }
  ASSERT_EQ(part.Finalize(&leaves), Status::kSuccess);
  EXPECT_GE(part.SkewFallbackCount(), 1U);
  EXPECT_LT(part.MaxObservedDepth(), 8U);

  uint64_t heavy_rows = 0;
  uint64_t total = 0;
  for (const PartitionLeaf& leaf : leaves.leaves) {
    if (leaf.skew_fallback) {
      heavy_rows += leaf.row_count;
    }
    ASSERT_EQ(RecursivePartitioner::ReadLeafRows(&store, leaf,
                                                 [&](Row&&) {
                                                   ++total;
                                                   return Status::kSuccess;
                                                 }),
              Status::kSuccess);
  }
  EXPECT_GT(heavy_rows, 2000U);
  EXPECT_EQ(total, static_cast<uint64_t>(kRows));
  leaves.DropAll();
  EXPECT_EQ(store.Stats().live_segments, 0U);
}

TEST(RecursivePartitionerTest, FailedSpillTransitionKeepsRowsForRetry) {
  for (bool retry : {false, true}) {
    SCOPED_TRACE(retry);
    TempDirGuard dir("ooc_part_transition_retry");
    SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
    RecursivePartitioner part(
        RecursivePartitioner::Options{.spill_store = &store,
                                      .memory_budget_bytes = 44ULL << 10},
        [](const Row&) { return uint64_t{0}; });
    const std::string text(6000, 'x');
    for (int64_t i = 0; i < 7; ++i) {
      ASSERT_EQ(part.AddRow(Row({Value(i), Value(std::string(text))})),
                Status::kSuccess);
    }
    ASSERT_FALSE(part.Spilled());
    const Row incoming({Value(7), Value(std::string(text))});
    auto& budget = ProcessMemoryBudget::Global();
    budget.SetTestSpillIoFailCountdown(2);
    const Status failed = part.AddRow(incoming);
    budget.SetTestSpillIoFailCountdown(0);
    ASSERT_EQ(failed, StatusCode::kIOError);
    EXPECT_FALSE(part.Spilled());
    EXPECT_EQ(part.TotalRows(), 7U);
    EXPECT_EQ(store.Stats().live_segments, 0U);
    if (retry) {
      ASSERT_EQ(part.AddRow(incoming), Status::kSuccess);
      ASSERT_TRUE(part.Spilled());
      EXPECT_EQ(part.TotalRows(), 8U);
    }
    PartitionLeafSet leaves;
    ASSERT_EQ(part.Finalize(&leaves), Status::kSuccess);
    std::vector<int64_t> ids;
    for (const PartitionLeaf& leaf : leaves.leaves) {
      ASSERT_EQ(RecursivePartitioner::ReadLeafRows(
                    &store, leaf,
                    [&](Row&& row) {
                      if (row.Count() != 2) {
                        return Status::kCorrupt;
                      }
                      EXPECT_EQ(row[1].AsStringView(), text);
                      ids.push_back(row[0].AsInt64());
                      return Status::kSuccess;
                    }),
                Status::kSuccess);
    }
    std::vector<int64_t> expected{0, 1, 2, 3, 4, 5, 6};
    if (retry) {
      expected.push_back(7);
    }
    EXPECT_EQ(ids, expected);
  }
}

TEST(RecursivePartitionerTest, FaultInjectionDropsPartialBucketsCleanly) {
  TempDirGuard dir("ooc_part_fault");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  ProcessMemoryBudget& mem = ProcessMemoryBudget::Global();
  struct Cleanup {
    explicit Cleanup(ProcessMemoryBudget* budget) : m(budget) {}
    Cleanup(const Cleanup&) = delete;
    Cleanup& operator=(const Cleanup&) = delete;
    Cleanup(Cleanup&&) = delete;
    Cleanup& operator=(Cleanup&&) = delete;
    ~Cleanup() { m->SetTestSpillIoFailCountdown(0); }
    ProcessMemoryBudget* m;
  } cleanup(&mem);

  {
    PartitionLeafSet leaves;
    RecursivePartitioner part(
        RecursivePartitioner::Options{.spill_store = &store,
                                      .memory_budget_bytes = 64ULL << 10,
                                      .force_spill = true},
        [](const Row& r) {
          return static_cast<uint64_t>(r[0].AsInt64()) << 63;
        });
    ASSERT_EQ(part.AddRow(Row({Value(int64_t{0})})), Status::kSuccess);
    ASSERT_EQ(part.AddRow(Row({Value(int64_t{1})})), Status::kSuccess);
    // The two rows use separate root buckets and stay buffered until
    // Finalize. Fail the second write after sealing the first bucket.
    mem.SetTestSpillIoFailCountdown(2);
    const Status st = part.Finalize(&leaves);
    EXPECT_EQ(st.GetCode(), StatusCode::kIOError);
    EXPECT_TRUE(leaves.leaves.empty());
    part.Reset();
    EXPECT_EQ(store.Stats().live_segments, 0U);
  }
  EXPECT_EQ(store.Stats().current_active_bytes, 0U);
}

TEST(RecursivePartitionerTest, FinalizeRetriesFailedRootWriteAndSplitRead) {
  for (uint64_t fail_io : {1, 2}) {
    SCOPED_TRACE(fail_io);
    TempDirGuard dir("ooc_part_finalize_retry");
    SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
    RecursivePartitioner part(
        RecursivePartitioner::Options{.spill_store = &store,
                                      .memory_budget_bytes = 16ULL << 10,
                                      .force_spill = true},
        [](const Row& row) {
          const auto id = static_cast<uint64_t>(row[0].AsInt64());
          return id == 3 ? id << 62 : id << 60;
        });
    const std::string payload(static_cast<size_type>(9 * 1024), 'x');
    for (int64_t id : {0, 1, 3}) {
      ASSERT_EQ(part.AddRow(Row({Value(id), Value(std::string(payload))})),
                Status::kSuccess);
    }

    PartitionLeafSet leaves;
    leaves.leaves.emplace_back().row_count = 99;
    auto& budget = ProcessMemoryBudget::Global();
    struct ResetSpillIoFailure {
      ProcessMemoryBudget& budget;
      ~ResetSpillIoFailure() { budget.SetTestSpillIoFailCountdown(0); }
    } reset_spill_io_failure{budget};
    // The large bucket is already written. Fail the small bucket's write
    // or the first read of the large bucket after completing the small leaf.
    budget.SetTestSpillIoFailCountdown(fail_io);
    const Status failed = part.Finalize(&leaves);
    ASSERT_EQ(failed.GetCode(), StatusCode::kIOError);
    EXPECT_NE(failed.GetMessage().find(fail_io == 1 ? "write" : "read"),
              std::string::npos);
    ASSERT_EQ(leaves.leaves.size(), 1U);
    EXPECT_EQ(leaves.leaves.front().row_count, 99U);
    EXPECT_EQ(part.AddRow(Row({Value(4)})), StatusCode::kInvalidArgument);

    ASSERT_EQ(part.Finalize(&leaves), Status::kSuccess);
    std::vector<int64_t> ids;
    for (const PartitionLeaf& leaf : leaves.leaves) {
      ASSERT_EQ(RecursivePartitioner::ReadLeafRows(
                    &store, leaf,
                    [&](Row&& row) {
                      EXPECT_EQ(row[1].AsStringView(), payload);
                      ids.push_back(row[0].AsInt64());
                      return Status::kSuccess;
                    }),
                Status::kSuccess);
    }
    std::ranges::sort(ids);
    EXPECT_EQ(ids, (std::vector<int64_t>{0, 1, 3}));
    leaves.DropAll();
    EXPECT_EQ(store.Stats().live_segments, 0U);
  }
}

TEST(OocPrimitivesSorterTest, ShouldSkipTopKChunkRowRespectsCutoffs) {
  TempDirGuard dir("ooc_sorter_topk_skip");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  ExternalSorter sorter(ExternalSorter::Options{
      .spill_store = &store,
      .memory_budget_bytes = 2ULL << 20,
      .keys = {OocSortKey{.column_index = 0, .ascending = false},
               OocSortKey{.column_index = 1, .ascending = true}},
      .limit = 3,
  });

  // Fill top-3 DESC past `*options_.limit` so `in_limit_heap_` activates with
  // {40, 30, 20} (cutoff root == 20).
  int64_t seq = 0;
  for (const int64_t v : {10, 30, 20, 40}) {
    ASSERT_EQ(sorter.AddRow(Row({Value(v), Value(seq++)})), Status::kSuccess);
  }

  DataChunk probe;
  probe.Reserve(4);
  probe.Append(Row({Value(int64_t{15}), Value(seq++)}));  // 15 < 20 -> skip
  probe.Append(Row({Value(int64_t{20}),
                    Value(seq++)}));  // tie == 20 -> skip (later tie-breaker
                                      // seq; all SQL keys compare equal)
  probe.Append(Row({Value(int64_t{25}), Value(seq++)}));  // 25 > 20 -> keep
  probe.Append(
      Row({Value(), Value(seq++)}));  // NULL (NULLS LAST in DESC) -> skip

  EXPECT_TRUE(sorter.ShouldSkipTopKChunkRow(probe, 0, 1));
  EXPECT_TRUE(sorter.ShouldSkipTopKChunkRow(probe, 1, 1));
  EXPECT_FALSE(sorter.ShouldSkipTopKChunkRow(probe, 2, 1));
  EXPECT_TRUE(sorter.ShouldSkipTopKChunkRow(probe, 3, 1));

  // Add non-skipped rows cleanly, finish, and verify exact descending Top-3.
  ASSERT_EQ(sorter.AddRow(probe.RowAt(2)), Status::kSuccess);
  ASSERT_EQ(sorter.Finish(), Status::kSuccess);
  Row out;
  std::vector<int64_t> actual;
  StatusOr<bool> has = sorter.NextRow(&out);
  while (has.HasValue() && has.Value()) {
    actual.push_back(out[0].AsInt64());
    has = sorter.NextRow(&out);
  }
  ASSERT_EQ(has.GetStatus(), Status::kSuccess);
  EXPECT_EQ(actual, (std::vector<int64_t>{40, 30, 25}));
}

TEST(SpillableStoreTest, AppendRowStagingRespectsTightGrantAndMoveAppendWorks) {
  TempDirGuard dir("ooc_spillable_tight_grant");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
  // non-copyable by design
  struct ResetGuard {
    explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } guard(&mb);
  mb.ResetForTest(64ULL << 20, 64ULL << 20, 64ULL << 20, false);
  QueryMemoryCoordinator coord(&mb);
  auto grant_or = coord.AcquireGrant(64ULL << 10, 64ULL << 10);
  ASSERT_TRUE(grant_or.HasValue()) << grant_or.GetStatus();
  auto grant = grant_or.MoveValue();

  // Another concurrent consumer holds almost all of the grant.
  ASSERT_TRUE(grant.TryConsume(60ULL << 10));
  {
    SpillableStore sstore(SpillableStore::Options{
        .spill_store = &store,
        .memory_budget_bytes = 64ULL << 10,
        .grant = &grant,
    });
    const std::string payload(1024, 'R');
    for (int i = 0; i < 16; ++i) {
      ASSERT_EQ(sstore.AppendRow(
                    Row({Value(int64_t{i}), Value(std::string(payload))})),
                Status::kSuccess);
      EXPECT_LE(grant.UsedBytes(), grant.GrantedBytes());
    }
    DataChunk moved_chunk;
    moved_chunk.Append(Row({Value(int64_t{99}), Value("moved")}));
    ASSERT_EQ(sstore.AppendChunk(std::move(moved_chunk)), Status::kSuccess);
    ASSERT_EQ(sstore.Seal(), Status::kSuccess);
    EXPECT_TRUE(sstore.Spilled());

    ASSERT_EQ(sstore.ResetCursor(), Status::kSuccess);
    int64_t count = 0;
    Row out;
    StatusOr<bool> has = sstore.NextRow(&out);
    while (has.HasValue() && has.Value()) {
      ++count;
      has = sstore.NextRow(&out);
    }
    ASSERT_EQ(has.GetStatus(), Status::kSuccess);
    EXPECT_EQ(count, 17);
  }
  grant.ReleaseConsumed(60ULL << 10);
  EXPECT_EQ(grant.UsedBytes(), 0U);
}

TEST(SpillableStoreTest, FailedStagingFlushRetainsChargeAndRows) {
  TempDirGuard dir("staging_flush_charge");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  auto& budget = ProcessMemoryBudget::Global();
  QueryMemoryCoordinator coord(&budget);
  auto acquired = coord.AcquireGrant(256ULL << 10, 256ULL << 10);
  ASSERT_TRUE(acquired.HasValue());
  auto grant = acquired.MoveValue();
  SpillableStore rows({.spill_store = &store, .memory_budget_bytes = 256ULL << 10,
                       .grant = &grant, .io_buffer_bytes = 0, .force_spill = true});
  const Row expected({Value(std::string(8192, 'x'))});
  ASSERT_EQ(rows.AppendRow(expected), Status::kSuccess);
  const auto before = grant.UsedBytes();
  ASSERT_GT(before, 0U);
  budget.SetTestSpillIoFailCountdown(1);
  const Status st = rows.Seal();
  budget.SetTestSpillIoFailCountdown(0);
  ASSERT_EQ(st, Status::kIOError);
  EXPECT_EQ(grant.UsedBytes(), before);
  ASSERT_EQ(rows.Seal(), Status::kSuccess);
  Row row;
  const auto first = rows.NextRow(&row);
  ASSERT_TRUE(first.HasValue() && first.Value());
  EXPECT_EQ(row, expected);
  const auto end = rows.NextRow(&row);
  ASSERT_TRUE(end.HasValue());
  EXPECT_FALSE(end.Value());
}

TEST(SpillableStoreTest, SpilledRowsChargeGrantOrFlushImmediately) {
  TempDirGuard dir("ooc_spillable_spilled_staging");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  struct ResetGuard {
    ProcessMemoryBudget* budget;
    ~ResetGuard() { budget->ResetForTest(0); }
  } guard{&mb};
  mb.ResetForTest(64ULL << 20, 64ULL << 20, 64ULL << 20, false);
  QueryMemoryCoordinator coord(&mb);
  auto grant_or = coord.AcquireGrant(64ULL << 10, 64ULL << 10);
  ASSERT_TRUE(grant_or.HasValue()) << grant_or.GetStatus();
  auto grant = grant_or.MoveValue();
  {
    SpillableStore sstore(SpillableStore::Options{
        .spill_store = &store,
        .memory_budget_bytes = 64ULL << 10,
        .grant = &grant,
        .force_spill = true,
    });
    DataChunk first;
    first.Append(Row({Value("first")}));
    ASSERT_EQ(sstore.AppendChunk(std::move(first)), Status::kSuccess);
    ASSERT_TRUE(sstore.Spilled());
    ASSERT_EQ(sstore.AppendRow(Row({Value("staged")})), Status::kSuccess);
    EXPECT_GT(grant.UsedBytes(), 0U);
    const std::string large(128ULL << 10, 'x');
    ASSERT_EQ(sstore.AppendRow(Row({Value(std::string(large))})),
              Status::kSuccess);
    ASSERT_EQ(sstore.Seal(), Status::kSuccess);
    ASSERT_EQ(sstore.ResetCursor(), Status::kSuccess);
    Row out;
    auto row1 = sstore.NextRow(&out);
    ASSERT_EQ(row1.GetStatus(), Status::kSuccess);
    ASSERT_TRUE(row1.Value());
    auto row2 = sstore.NextRow(&out);
    ASSERT_EQ(row2.GetStatus(), Status::kSuccess);
    ASSERT_TRUE(row2.Value());
    auto row3 = sstore.NextRow(&out);
    ASSERT_EQ(row3.GetStatus(), Status::kSuccess);
    ASSERT_TRUE(row3.Value());
    EXPECT_TRUE(out[0].AsStringView() == large);
    auto end = sstore.NextRow(&out);
    ASSERT_EQ(end.GetStatus(), Status::kSuccess);
    EXPECT_FALSE(end.Value());
  }
  EXPECT_EQ(grant.UsedBytes(), 0U);
}

TEST(RecursivePartitionerTest,
     RadixBucketAtOffsetPreventsOverlapAcrossBudgetShifts) {
  // Root consumed top 6 MSB bits [58, 64); child uses 2 bits at offset 6 ->
  // [56, 58), NOT overlapping [60, 62).
  const uint64_t h0 = uint64_t{0b00} << 56;
  const uint64_t h1 = uint64_t{0b01} << 56;
  const uint64_t h2 = uint64_t{0b10} << 56;
  const uint64_t h3 = uint64_t{0b11} << 56;
  EXPECT_EQ(RecursivePartitioner::RadixBucketAtOffset(h0, 6, 2), 0U);
  EXPECT_EQ(RecursivePartitioner::RadixBucketAtOffset(h1, 6, 2), 1U);
  EXPECT_EQ(RecursivePartitioner::RadixBucketAtOffset(h2, 6, 2), 2U);
  EXPECT_EQ(RecursivePartitioner::RadixBucketAtOffset(h3, 6, 2), 3U);

  // Also exercise a dynamic grant change between AddRow() and Finalize().
  // The split fanout stays fixed at the root's value; see the note below.
  TempDirGuard dir("ooc_part_dynamic_bits");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
  // non-copyable by design
  struct ResetGuard {
    explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } guard(&mb);
  mb.ResetForTest(64ULL << 20, 64ULL << 20, 64ULL << 20, false);
  QueryMemoryCoordinator coord(&mb);
  auto grant_or = coord.AcquireGrant(2ULL << 20, 2ULL << 20);
  ASSERT_TRUE(grant_or.HasValue()) << grant_or.GetStatus();
  auto grant = grant_or.MoveValue();

  PartitionLeafSet leaves;
  RecursivePartitioner part(
      RecursivePartitioner::Options{.spill_store = &store,
                                    .memory_budget_bytes = 2ULL << 20,
                                    .grant = &grant,
                                    .force_spill = true},
      [](const Row& r) {
        // Spread across 4 root buckets (`[59..61)` via bits 59..60 -> 25% each
        // so root never trips the >80% skew guard), and uniform across child
        // bits `[55..59)` (`[57..59)` then `[55..57)`).
        const auto v = static_cast<uint64_t>(r[0].AsInt64());
        return ((v & 0x3ULL) << 59) | (((v >> 2) & 0xFULL) << 55);
      });
  const std::string pad(512, 'B');
  for (int i = 0; i < 4200; ++i) {
    ASSERT_EQ(part.AddRow(Row({Value(int64_t{i}), Value(std::string(pad))})),
              Status::kSuccess);
  }
  // Shrink the operator budget drastically right before Finalize() splits.
  // Finalize() deliberately discards ComputeFanoutBits for the new budget and
  // keeps child_fanout_bits = root_fanout_bits_, so a budget change only
  // re-sizes the writer buffers; it does NOT change the child fanout (that
  // fixed fanout is what preserves the refinement guarantee).
  std::vector<QueryMemoryGrant::ActiveConsumerGuard> consumers;
  consumers.reserve(7);
  for (int i = 0; i < 7; ++i) {
    consumers.emplace_back(&grant);
  }
  ASSERT_EQ(part.Finalize(&leaves), Status::kSuccess);
  EXPECT_EQ(part.SkewFallbackCount(), 0U);
  EXPECT_GT(leaves.leaves.size(), 1U);
  uint64_t total = 0;
  for (const PartitionLeaf& leaf : leaves.leaves) {
    ASSERT_EQ(RecursivePartitioner::ReadLeafRows(&store, leaf,
                                                 [&](Row&&) {
                                                   ++total;
                                                   return Status::kSuccess;
                                                 }),
              Status::kSuccess);
  }
  EXPECT_EQ(total, 4200U);
}

TEST(OocPrimitivesSorterTest,
     GrantExhaustionDuringAddRowSpillsSingleRowRunSafely) {
  TempDirGuard dir("ooc_sorter_grant_exhaustion");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
  // non-copyable by design
  struct ResetGuard {
    explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } guard(&mb);
  mb.ResetForTest(64ULL << 20, 64ULL << 20, 64ULL << 20, false);
  QueryMemoryCoordinator coord(&mb);
  auto grant_or = coord.AcquireGrant(256ULL << 10, 256ULL << 10);
  ASSERT_TRUE(grant_or.HasValue()) << grant_or.GetStatus();
  auto grant = grant_or.MoveValue();

  ExternalSorter sorter(ExternalSorter::Options{
      .spill_store = &store,
      .memory_budget_bytes = 256ULL << 10,
      .grant = &grant,
      .keys = {OocSortKey{.column_index = 0, .ascending = true}},
      .limit = std::nullopt,
  });
  ASSERT_EQ(sorter.AddRow(Row({Value(int64_t{2}), Value("small")})),
            Status::kSuccess);

  // Consume all remaining grant after the sorter's initial 64 KiB refill so
  // the next row hits an unchargeable reserve. Keep it below B_op/4 (64 KiB)
  // so ADR-0004 rule 9 does not reject it before the grant path is reached.
  const size_t rem = grant.GrantedBytes() - grant.UsedBytes();
  ASSERT_GT(rem, 0U);
  ASSERT_TRUE(grant.TryConsume(rem));
  const std::string big(8ULL << 10, 'X');
  ASSERT_EQ(sorter.AddRow(Row({Value(int64_t{1}), Value(std::string(big))})),
            Status::kSuccess);
  EXPECT_LE(grant.UsedBytes(), grant.GrantedBytes());
  grant.ReleaseConsumed(rem);

  ASSERT_EQ(sorter.Finish(), Status::kSuccess);
  Row out;
  auto first = sorter.NextRow(&out);
  ASSERT_EQ(first.GetStatus(), Status::kSuccess);
  ASSERT_TRUE(first.Value());
  EXPECT_EQ(out[0].AsInt64(), 1);
  auto second = sorter.NextRow(&out);
  ASSERT_EQ(second.GetStatus(), Status::kSuccess);
  ASSERT_TRUE(second.Value());
  EXPECT_EQ(out[0].AsInt64(), 2);
  auto end = sorter.NextRow(&out);
  ASSERT_EQ(end.GetStatus(), Status::kSuccess);
  EXPECT_FALSE(end.Value());
}

}  // namespace
}  // namespace tinylamb
