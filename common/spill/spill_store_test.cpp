/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/spill/spill_store.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/platform/file_io.hpp"
#include "common/random_string.hpp"

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

TEST(SpillStoreTest, LazyOpenAndUnlinkedTempDirRemainsEmpty) {
  TempDirGuard dir("spill_store_lazy");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  EXPECT_EQ(store.Stats().open_fds, 0);
  auto seg = store.AllocateSegment();
  ASSERT_EQ(seg.GetStatus().GetCode(), StatusCode::kSuccess);
  EXPECT_EQ(store.Stats().open_fds, 0);
  EXPECT_EQ(store.Stats().live_segments, 1U);

  std::vector<uint8_t> payload(200000, 0x5A);
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<uint8_t>((i * 31 + 7) & 0xff);
  }
  ASSERT_EQ(store.Append(seg.Value(), payload.data(), payload.size()),
            Status::kSuccess);
  EXPECT_EQ(store.Stats().open_fds, 1);

  std::error_code ec;
  EXPECT_TRUE(std::filesystem::is_empty(dir.path, ec)) << ec.message();

  std::vector<uint8_t> out(payload.size(), 0);
  ASSERT_EQ(store.Read(seg.Value(), 0, out.data(), out.size()),
            Status::kSuccess);
  EXPECT_EQ(out, payload);

  // Also test partial offset reads across block boundary.
  std::vector<uint8_t> slice(5000, 0);
  const uint64_t off = SpillStore::kBlockSize - 1000;
  ASSERT_EQ(store.Read(seg.Value(), off, slice.data(), slice.size()),
            Status::kSuccess);
  EXPECT_TRUE(std::equal(slice.begin(), slice.end(), payload.begin() + off));

  store.DropSegment(seg.Value());
  EXPECT_EQ(store.Stats().current_active_bytes, 0U);
  EXPECT_EQ(store.Stats().physical_file_bytes, 0U);
}

TEST(SpillStoreTest, TenThousandSegmentsMaintainConstantOpenFds) {
  TempDirGuard dir("spill_store_10k");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  constexpr int kSegments = 10000;
  std::vector<SpillSegmentId> ids;
  ids.reserve(kSegments);
  for (int i = 0; i < kSegments; ++i) {
    auto id_or = store.AllocateSegment();
    ASSERT_EQ(id_or.GetStatus().GetCode(), StatusCode::kSuccess);
    const SpillSegmentId id = id_or.Value();
    ids.push_back(id);
    const uint64_t val = static_cast<uint64_t>(i) * 1000003ULL;
    ASSERT_EQ(store.Append(id, &val, sizeof(val)), Status::kSuccess);
  }

  const SpillStoreStats stats = store.Stats();
  EXPECT_EQ(stats.open_fds, 1);
  EXPECT_EQ(stats.live_segments, static_cast<uint64_t>(kSegments));

  for (size_t i = 0; i < static_cast<size_t>(kSegments); i += 97) {
    uint64_t val = 0;
    ASSERT_EQ(store.Read(ids[i], 0, &val, sizeof(val)), Status::kSuccess);
    EXPECT_EQ(val, static_cast<uint64_t>(i) * 1000003ULL);
  }

  for (const SpillSegmentId id : ids) {
    store.DropSegment(id);
  }
  EXPECT_EQ(store.Stats().live_segments, 0U);
  EXPECT_EQ(store.Stats().current_active_bytes, 0U);
  EXPECT_EQ(store.Stats().physical_file_bytes, 0U);
}

TEST(SpillStoreTest, FreeBlockReuseBoundsPhysicalFileGrowth) {
  TempDirGuard dir("spill_store_reuse");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  std::vector<uint8_t> block(SpillStore::kBlockSize, 0xAB);
  std::vector<SpillSegmentId> batch1;
  for (int i = 0; i < 6; ++i) {
    auto id_or = store.AllocateSegment();
    ASSERT_EQ(id_or.GetStatus().GetCode(), StatusCode::kSuccess);
    batch1.push_back(id_or.Value());
    ASSERT_EQ(store.Append(id_or.Value(), block.data(), block.size()),
              Status::kSuccess);
  }
  const uint64_t file_bytes_after_batch1 = store.Stats().physical_file_bytes;
  EXPECT_EQ(file_bytes_after_batch1, 6ULL * SpillStore::kBlockSize);

  // Drop 3 segments and allocate 3 replacements: all blocks must be reused.
  for (size_t i = 0; i < 3; ++i) {
    store.DropSegment(batch1[i]);
  }
  for (int i = 0; i < 3; ++i) {
    auto id_or = store.AllocateSegment();
    ASSERT_EQ(id_or.GetStatus().GetCode(), StatusCode::kSuccess);
    std::vector<uint8_t> data(SpillStore::kBlockSize,
                              static_cast<uint8_t>(i + 1));
    ASSERT_EQ(store.Append(id_or.Value(), data.data(), data.size()),
              Status::kSuccess);
    std::vector<uint8_t> read_back(SpillStore::kBlockSize, 0);
    ASSERT_EQ(store.Read(id_or.Value(), 0, read_back.data(), read_back.size()),
              Status::kSuccess);
    EXPECT_EQ(read_back, data);
  }
  const SpillStoreStats after_reuse = store.Stats();
  EXPECT_EQ(after_reuse.blocks_reused, 3U);
  EXPECT_EQ(after_reuse.physical_file_bytes, file_bytes_after_batch1);
}

TEST(SpillStoreTest, TempLimitEnforcesNoSpaceAndRecoversAfterDrop) {
  TempDirGuard dir("spill_store_limit");
  constexpr size_t kLimit = 4ULL * SpillStore::kBlockSize;  // 256 KiB
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path,
                                     .temp_limit_bytes = kLimit,
                                     .use_global_temp_limit = false});

  std::vector<uint8_t> two_blocks(2ULL * SpillStore::kBlockSize, 0x33);
  auto seg1 = store.AllocateSegment().MoveValue();
  auto seg2 = store.AllocateSegment().MoveValue();
  ASSERT_EQ(store.Append(seg1, two_blocks.data(), two_blocks.size()),
            Status::kSuccess);
  ASSERT_EQ(store.Append(seg2, two_blocks.data(), two_blocks.size()),
            Status::kSuccess);

  // Any further block allocation must fail cleanly with kNoSpace.
  auto seg3 = store.AllocateSegment().MoveValue();
  const Status over = store.Append(seg3, two_blocks.data(), 16);
  EXPECT_EQ(over.GetCode(), StatusCode::kNoSpace);
  EXPECT_EQ(store.Stats().current_active_bytes, kLimit);

  // Existing segments remain intact.
  std::vector<uint8_t> verify(two_blocks.size(), 0);
  ASSERT_EQ(store.Read(seg1, 0, verify.data(), verify.size()),
            Status::kSuccess);
  EXPECT_EQ(verify, two_blocks);

  // Dropping one segment frees space so `seg3` can succeed cleanly.
  store.DropSegment(seg2);
  ASSERT_EQ(store.Append(seg3, two_blocks.data(), two_blocks.size()),
            Status::kSuccess);
}

TEST(SpillStoreTest, InjectedIoFailureRollsBackWithoutLeakingBlocks) {
  TempDirGuard dir("spill_store_fault");
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
  } cleanup{&mem};

  {
    SpillSegmentWriter writer(&store, /*buffer_bytes=*/16384);
    ASSERT_EQ(writer.EnsureOpen(), Status::kSuccess);
    mem.SetTestSpillIoFailCountdown(1);
    std::vector<uint8_t> big(200000, 0x11);
    const Status st = writer.Write(big.data(), big.size());
    EXPECT_EQ(st.GetCode(), StatusCode::kIOError);
    // Destroy `writer` before Finish() -> auto-drops segment, leaking 0 blocks.
  }

  const SpillStoreStats after_abort = store.Stats();
  EXPECT_EQ(after_abort.live_segments, 0U);
  EXPECT_EQ(after_abort.current_active_bytes, 0U);

  // Mid-multi-slice failure (2nd block fails): verify full rollback too.
  {
    SpillSegmentWriter writer(&store, /*buffer_bytes=*/0);
    ASSERT_EQ(writer.EnsureOpen(), Status::kSuccess);
    mem.SetTestSpillIoFailCountdown(2);
    std::vector<uint8_t> two_blocks(SpillStore::kBlockSize + 4096, 0x22);
    const Status st = writer.Write(two_blocks.data(), two_blocks.size());
    EXPECT_EQ(st.GetCode(), StatusCode::kIOError);
  }
  EXPECT_EQ(store.Stats().live_segments, 0U);
  EXPECT_EQ(store.Stats().current_active_bytes, 0U);

  // Subsequent clean write succeeds normally.
  SpillSegmentWriter writer(&store, /*buffer_bytes=*/4096);
  std::vector<uint8_t> ok_data(150000, 0);
  std::iota(ok_data.begin(), ok_data.end(), uint8_t{1});
  ASSERT_EQ(writer.Write(ok_data.data(), ok_data.size()), Status::kSuccess);
  auto id_or = writer.Finish();
  ASSERT_EQ(id_or.GetStatus().GetCode(), StatusCode::kSuccess);

  SpillSegmentReader reader(&store, id_or.Value(), /*buffer_bytes=*/8192);
  std::vector<uint8_t> out(ok_data.size(), 0);
  ASSERT_EQ(reader.ReadExact(out.data(), out.size()), Status::kSuccess);
  EXPECT_TRUE(reader.Eof());
  EXPECT_EQ(out, ok_data);
}

TEST(SpillStoreTest, ConcurrentSegmentWritesAndReads) {
  TempDirGuard dir("spill_store_threads");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  constexpr int kThreads = 4;
  constexpr size_t kPayloadBytes = 180000;
  std::vector<SpillSegmentId> segs(kThreads, kInvalidSpillSegmentId);

  std::vector<std::thread> writers;
  writers.reserve(kThreads);
  for (size_t t = 0; t < static_cast<size_t>(kThreads); ++t) {
    writers.emplace_back([&, t]() {
      SpillSegmentWriter w(&store, 8192);
      std::vector<uint8_t> buf(kPayloadBytes);
      for (size_t i = 0; i < buf.size(); ++i) {
        buf[i] = static_cast<uint8_t>((i + t * 13) & 0xff);
      }
      ASSERT_EQ(w.Write(buf.data(), buf.size()), Status::kSuccess);
      auto res = w.Finish();
      ASSERT_EQ(res.GetStatus().GetCode(), StatusCode::kSuccess);
      segs[t] = res.Value();
    });
  }
  for (auto& th : writers) {
    th.join();
  }

  std::vector<std::thread> readers;
  readers.reserve(kThreads);
  for (size_t t = 0; t < static_cast<size_t>(kThreads); ++t) {
    readers.emplace_back([&, t]() {
      SpillSegmentReader r(&store, segs[t], 8192);
      std::vector<uint8_t> out(kPayloadBytes, 0);
      ASSERT_EQ(r.ReadExact(out.data(), out.size()), Status::kSuccess);
      EXPECT_TRUE(r.Eof());
      for (size_t i = 0; i < out.size(); ++i) {
        ASSERT_EQ(out[i], static_cast<uint8_t>((i + t * 13) & 0xff));
      }
    });
  }
  for (auto& th : readers) {
    th.join();
  }

  store.Clear();
  EXPECT_EQ(store.Stats().live_segments, 0U);
  EXPECT_EQ(store.Stats().current_active_bytes, 0U);
}

TEST(SpillStoreTest, ConcurrentDropAndClearDeferWhileAppendInFlight) {
  TempDirGuard dir("spill_store_concurrent_drop");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  constexpr size_t kLargeAppend = size_t{4} << 20;  // 4 MiB = 1024 blocks
  std::vector<uint8_t> payload(kLargeAppend, 0x5A);

  for (int iter = 0; iter < 8; ++iter) {
    const bool use_clear = (iter % 2) == 1;
    auto seg_or = store.AllocateSegment();
    ASSERT_EQ(seg_or.GetStatus().GetCode(), StatusCode::kSuccess);
    const SpillSegmentId seg = seg_or.Value();
    // Pre-populate 1 block so a concurrent DropSegment would overlap with
    // existing segment blocks if not deferred.
    std::vector<uint8_t> head(SpillStore::kBlockSize, 0x11);
    ASSERT_EQ(store.Append(seg, head.data(), head.size()), Status::kSuccess);

    std::atomic<bool> writer_done{false};
    Status append_status = Status::kSuccess;
    std::thread writer([&]() {
      append_status = store.Append(seg, payload.data(), payload.size());
      writer_done.store(true, std::memory_order_release);
    });

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!writer_done.load(std::memory_order_acquire) &&
           store.Stats().current_active_bytes <= SpillStore::kBlockSize &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    if (use_clear) {
      store.Clear();
    } else {
      store.DropSegment(seg);
    }
    writer.join();

    // Either writer completed before Drop/Clear took the lock (kSuccess) or
    // saw pending_drop afterward (kNotExists). In both cases no blocks may
    // leak or corrupt store state.
    EXPECT_TRUE(append_status == Status::kSuccess ||
                append_status.GetCode() == StatusCode::kNotExists);
    const SpillStoreStats st = store.Stats();
    EXPECT_EQ(st.live_segments, 0U);
    EXPECT_EQ(st.current_active_bytes, 0U);
    EXPECT_EQ(store.SegmentSize(seg).GetStatus().GetCode(),
              StatusCode::kNotExists);

    // Subsequent writes and reads must still work cleanly after deferred
    // cleanup.
    SpillSegmentWriter check_writer(&store, SpillStore::kBlockSize);
    const std::string check_msg = "after_concurrent_drop";
    ASSERT_EQ(check_writer.Write(check_msg), Status::kSuccess);
    auto check_seg_or = check_writer.Finish();
    ASSERT_EQ(check_seg_or.GetStatus().GetCode(), StatusCode::kSuccess);
    SpillSegmentReader check_reader(&store, check_seg_or.Value(),
                                    SpillStore::kBlockSize);
    std::string out(check_msg.size(), '\0');
    ASSERT_EQ(check_reader.ReadExact(out.data(), out.size()), Status::kSuccess);
    EXPECT_EQ(out, check_msg);
    store.DropSegment(check_seg_or.Value());
    EXPECT_EQ(store.Stats().current_active_bytes, 0U);
  }
}

// #1094: a sequential read must never re-advise an already-completed block.
// The old body re-advised ALL completed blocks at every 64 KiB boundary,
// turning a B-block segment read into B(B+1)/2 AdviseDontNeed syscalls
// (≈134M for a 1 GiB spill); each block is now advised exactly once.
TEST(SpillStoreTest, SequentialReadAdvisesEachBlockExactlyOnce) {
  if (!platform::kSupportsAdviseDontNeed) {
    GTEST_SKIP() << "AdviseDontNeed unsupported on this platform";
  }
  TempDirGuard dir("spill_store_advise_once");
  SpillStore store(SpillStoreOptions{.temp_dir = dir.path});

  constexpr size_t kBlocks = 8;
  std::vector<uint8_t> data(kBlocks * SpillStore::kBlockSize);
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<uint8_t>((i * 31 + 7) & 0xff);
  }
  auto seg_or = store.AllocateSegment();
  ASSERT_EQ(seg_or.GetStatus().GetCode(), StatusCode::kSuccess);
  ASSERT_EQ(store.Append(seg_or.Value(), data.data(), data.size()),
            Status::kSuccess);

  const uint64_t baseline = store.AdviseDontNeedCalls();
  SpillSegmentReader reader(&store, seg_or.Value(), 16384);
  std::vector<uint8_t> out(data.size());
  for (size_t off = 0; off < data.size(); off += 4096) {
    const size_t len = std::min<size_t>(4096, data.size() - off);
    ASSERT_EQ(reader.ReadExact(out.data() + off, len), Status::kSuccess);
  }
  EXPECT_EQ(out, data);
  // Exactly one advisory per newly completed block, despite 8 separate
  // boundary crossings during the 128 small reads.  The last block stays
  // cached because the reader stops before a refill could start at EOF
  // (unchanged from the pre-fix boundary semantics: Refill computes the
  // boundary from the read START offset, so 7 of 8 blocks complete).
  EXPECT_EQ(store.AdviseDontNeedCalls() - baseline, kBlocks - 1);

  // Re-scan (the Band/Cross right-store pattern): nothing re-advises, and
  // the data still reads back intact.
  ASSERT_EQ(reader.Seek(0), Status::kSuccess);
  for (size_t off = 0; off < data.size(); off += 4096) {
    const size_t len = std::min<size_t>(4096, data.size() - off);
    ASSERT_EQ(reader.ReadExact(out.data() + off, len), Status::kSuccess);
  }
  EXPECT_EQ(out, data);
  EXPECT_EQ(store.AdviseDontNeedCalls(), baseline + kBlocks - 1);
}

}  // namespace
}  // namespace tinylamb
