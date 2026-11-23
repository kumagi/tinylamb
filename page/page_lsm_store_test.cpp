/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "page/page_lsm_store.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <mutex>
#include <new>

#include "common/memory_budget.hpp"
#include "common/platform/file_io.hpp"

namespace {

thread_local int allocations_before_failure = -1;

}  // namespace

// Inject a single allocation failure in this test executable and thread.
void* operator new(std::size_t bytes) {
  if (allocations_before_failure == 0) {
    allocations_before_failure = -1;
    throw std::bad_alloc();
  }
  if (allocations_before_failure > 0) {
    --allocations_before_failure;
  }
  if (void* result = std::malloc(bytes == 0 ? 1 : bytes)) {
    return result;
  }
  throw std::bad_alloc();
}

void operator delete(void* ptr) noexcept { std::free(ptr); }
void operator delete(void* ptr, std::size_t /*unused*/) noexcept { std::free(ptr); }

namespace tinylamb {
namespace {

TEST(LsmRunWriterTest, RejectsEntryLargerThanReadableBlock) {
  std::mutex mutex;
  uint64_t offset = 0, count = 0;
  bool tombstones = false, emitted = false;
  std::vector<LsmRunBlockMeta> blocks;
  const Status result = WriteLsmRunStream(
      -1, mutex, &offset,
      [&](LsmRunEntry* entry) {
        if (emitted) {
          return false;
        }
        emitted = true;
        entry->key = "k";
        entry->value.assign(size_t{64} << 20, 'x');
        return true;
      },
      &blocks, &count, &tombstones);
  EXPECT_EQ(result, Status::kTooBigData);
  EXPECT_TRUE(blocks.empty());
  EXPECT_EQ(offset, 0U);
}

TEST(LsmBlockCacheTest, AllocationFailureReturnsStatusAndLeavesCacheUsable) {
  struct TempRun {
    int fd = platform::OpenUnlinkedTempFile();
    ~TempRun() {
      if (fd >= 0) {
        platform::CloseFile(fd);
      }
    }
  } run;
  ASSERT_GE(run.fd, 0);
  std::mutex file_mu;
  uint64_t end = 0;
  uint64_t count = 0;
  bool tombstones = false;
  std::vector<LsmRunBlockMeta> blocks;
  bool wrote = false;
  const std::string payload(128, 'v');
  ASSERT_EQ(WriteLsmRunStream(
                run.fd, file_mu, &end,
                [&](LsmRunEntry* entry) {
                  if (wrote) {
                    return false;
                  }
                  *entry = LsmRunEntry{.key = "key", .value = payload};
                  wrote = true;
                  return true;
                },
                &blocks, &count, &tombstones),
            Status::kSuccess);
  ASSERT_EQ(blocks.size(), 1U);

  const size_t initial_storage = ProcessMemoryBudget::Global().StorageUsed();
  for (int fail_after = 0; fail_after < 32; ++fail_after) {
    SCOPED_TRACE(fail_after);
    LsmBlockCache cache;
    auto attempted = [&] {
      struct ResetFailure {
        ~ResetFailure() { allocations_before_failure = -1; }
      } reset_failure;
      allocations_before_failure = fail_after;
      return cache.GetOrLoad(run.fd, file_mu, 1, 0, blocks.front(), 64 << 10,
                             /*populate_cache=*/true);
    }();
    if (attempted.HasValue()) {
      EXPECT_GE(fail_after, 5);
      ASSERT_NE(attempted.Value(), nullptr);
      return;
    }
    EXPECT_EQ(attempted.GetStatus(), Status::kMemoryLimitExceeded);
    EXPECT_EQ(cache.CachedBytes(), 0U);
    EXPECT_EQ(ProcessMemoryBudget::Global().StorageUsed(), initial_storage);
    const auto retry =
        cache.GetOrLoad(run.fd, file_mu, 1, 0, blocks.front(), 64 << 10, true);
    ASSERT_TRUE(retry.HasValue()) << retry.GetStatus();
    ASSERT_NE(retry.Value(), nullptr);
    ASSERT_NE(retry.Value()->Find("key"), nullptr);
    EXPECT_EQ(retry.Value()->Find("key")->value, payload);
    cache.Clear();
    EXPECT_EQ(ProcessMemoryBudget::Global().StorageUsed(), initial_storage);
  }
  ADD_FAILURE() << "the cache did not populate after all injected failures";
}

// T4094: Current() must not dereference a reset block_ (or index past the
// last decoded entry) when a consumer forgets to check Valid().
TEST(LsmRunCursorTest, CurrentOnInvalidCursorYieldsEmptyEntry) {
  std::mutex file_mu;
  LsmBlockCache cache;
  const int fd = -1;
  LsmRunCursor cursor(fd, file_mu, cache, /*run_id=*/1, /*blocks=*/{},
                      /*cache_limit_bytes=*/64 << 10,
                      /*populate_cache=*/false);
  ASSERT_FALSE(cursor.Valid());
  const LsmRunEntry& entry = cursor.Current();
  EXPECT_TRUE(entry.key.empty());
  EXPECT_TRUE(entry.value.empty());
  EXPECT_FALSE(entry.deleted);
}

}  // namespace
}  // namespace tinylamb
