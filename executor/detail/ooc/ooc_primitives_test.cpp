/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <limits>
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
  auto grant = coord.AcquireGrant(512ULL << 10, 512ULL << 10).MoveValue();

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
  while (sorter.NextRow(&r).Value()) {
    out.push_back(r);
  }
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
  while (topn.NextRow(&r).Value()) {
    EXPECT_EQ(r[0].AsInt64(), expected++);
  }
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
  while (sorter.NextRow(&r).Value()) {
    const std::string_view payload = r[1].AsStringView();
    const int64_t idx = std::strtoll(std::string(payload).c_str(), nullptr, 10);
    EXPECT_EQ(idx, next_payload++)
        << "row " << next_payload - 1 << " lost run-order stability";
  }
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
                                      .memory_budget_bytes = 64ULL << 10},
        [](const Row& r) { return static_cast<uint64_t>(r[0].AsInt64()); });
    const std::string pad(200, 'F');
    for (int i = 0; i < 600; ++i) {
      ASSERT_EQ(part.AddRow(Row({Value(int64_t{i}), Value(std::string(pad))})),
                Status::kSuccess);
    }
    mem.SetTestSpillIoFailCountdown(1);
    const Status st = part.Finalize(&leaves);
    EXPECT_EQ(st.GetCode(), StatusCode::kIOError);
    EXPECT_EQ(store.Stats().live_segments, 0U);
  }
  EXPECT_EQ(store.Stats().current_active_bytes, 0U);
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
  probe.Append(Row({Value(int64_t{20}), Value(seq++)}));  // tie == 20 -> skip (later tie-breaker seq; all SQL keys compare equal)
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
  while (sorter.NextRow(&out).Value()) {
    actual.push_back(out[0].AsInt64());
  }
  EXPECT_EQ(actual, (std::vector<int64_t>{40, 30, 25}));
}

TEST(SpillableStoreTest, AppendRowStagingRespectsTightGrantAndMoveAppendWorks) {
  TempDirGuard dir("ooc_spillable_tight_grant");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is non-copyable by design
  struct ResetGuard {
    explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } guard(&mb);
  mb.ResetForTest(64ULL << 20, 64ULL << 20, 64ULL << 20, false);
  QueryMemoryCoordinator coord(&mb);
  auto grant = coord.AcquireGrant(64ULL << 10, 64ULL << 10).MoveValue();

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
    while (sstore.NextRow(&out).Value()) {
      ++count;
    }
    EXPECT_EQ(count, 17);
  }
  grant.ReleaseConsumed(60ULL << 10);
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

  // Also test dynamic grant change between AddRow() and Finalize() forcing
  // fanout reduction from 6 bits to 2 bits.
  TempDirGuard dir("ooc_part_dynamic_bits");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is non-copyable by design
  struct ResetGuard {
    explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } guard(&mb);
  mb.ResetForTest(64ULL << 20, 64ULL << 20, 64ULL << 20, false);
  QueryMemoryCoordinator coord(&mb);
  auto grant = coord.AcquireGrant(2ULL << 20, 2ULL << 20).MoveValue();

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
  // Shrink operator budget drastically right before Finalize() splits so
  // ComputeFanoutBits changes from 5 bits (root) to 2 bits (children).
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

TEST(OocPrimitivesSorterTest, GrantExhaustionDuringAddRowSpillsSingleRowRunSafely) {
  TempDirGuard dir("ooc_sorter_grant_exhaustion");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});
  ProcessMemoryBudget& mb = ProcessMemoryBudget::Global();
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is non-copyable by design
  struct ResetGuard {
    explicit ResetGuard(ProcessMemoryBudget* b) : budget(b) {}
    ~ResetGuard() { budget->ResetForTest(0); }
    ProcessMemoryBudget* budget;
  } guard(&mb);
  mb.ResetForTest(64ULL << 20, 64ULL << 20, 64ULL << 20, false);
  QueryMemoryCoordinator coord(&mb);
  auto grant = coord.AcquireGrant(256ULL << 10, 256ULL << 10).MoveValue();

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
  ASSERT_TRUE(sorter.NextRow(&out).Value());
  EXPECT_EQ(out[0].AsInt64(), 1);
  ASSERT_TRUE(sorter.NextRow(&out).Value());
  EXPECT_EQ(out[0].AsInt64(), 2);
  EXPECT_FALSE(sorter.NextRow(&out).Value());
}

}  // namespace
}  // namespace tinylamb
