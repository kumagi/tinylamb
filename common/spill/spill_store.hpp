/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_SPILL_SPILL_STORE_HPP
#define TINYLAMB_COMMON_SPILL_SPILL_STORE_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status_or.hpp"

namespace tinylamb {

using SpillSegmentId = uint64_t;
inline constexpr SpillSegmentId kInvalidSpillSegmentId = 0;

struct SpillStoreOptions {
  // Optional directory for unlinked spill backing files. When empty, reads
  // `TINYLAMB_TEMP_DIR` if set, otherwise uses OS temporary directory.
  std::filesystem::path temp_dir;
  // Optional explicit temp limit in bytes. When 0 and `use_global_temp_limit`
  // is true, reads `ProcessMemoryBudget::Global().TempLimitBytes()`.
  size_t temp_limit_bytes{0};
  bool use_global_temp_limit{true};
};

struct SpillStoreStats {
  uint64_t bytes_written{0};
  uint64_t bytes_read{0};
  uint64_t current_active_bytes{0};
  uint64_t peak_active_bytes{0};
  uint64_t physical_file_bytes{0};
  uint64_t peak_physical_file_bytes{0};
  uint64_t segments_created{0};
  uint64_t segments_dropped{0};
  uint64_t live_segments{0};
  uint64_t blocks_reused{0};
  int open_fds{0};
};

// Block-multiplexed temporary storage manager (ADR 0005).
//
// Maintains constant open file descriptors (at most one unlinked temp file)
// regardless of how many logical spill segments (partitions, sorted runs,
// hash buckets) exist simultaneously. Space from dropped segments is recycled
// at 64 KiB block granularity via an internal free list.
class SpillStore {
 public:
  static constexpr size_t kBlockSize = 64ULL * 1024ULL;

  explicit SpillStore(SpillStoreOptions options = {});
  ~SpillStore();

  SpillStore(const SpillStore&) = delete;
  SpillStore& operator=(const SpillStore&) = delete;
  SpillStore(SpillStore&&) = delete;
  SpillStore& operator=(SpillStore&&) = delete;

  // Allocates a new logical spill segment without opening files or reserving
  // disk blocks until data is actually appended.
  [[nodiscard]] StatusOr<SpillSegmentId> AllocateSegment();

  // Releases all blocks owned by `id` back to the free-block pool. Safe to call
  // with `kInvalidSpillSegmentId` or an already-dropped ID.
  void DropSegment(SpillSegmentId id) noexcept;

  // Appends `len` bytes to logical segment `id`. Checks temp-space limits
  // before reserving new blocks and rolls back newly reserved blocks on any
  // I/O or disk-space error.
  [[nodiscard]] Status Append(SpillSegmentId id, const void* data, size_t len);

  // Reads exactly `len` bytes from logical segment `id` starting at `offset`.
  // Safe for concurrent readers on finalized segments. A concurrent
  // DropSegment/Clear defers block reclamation until in-flight reads finish,
  // so a read never observes reused blocks or a truncated file.
  [[nodiscard]] Status Read(SpillSegmentId id, uint64_t offset, void* dst,
                            size_t len) const;

  // Best-effort kernel page-cache release for completed blocks strictly before
  // `up_to_offset` in segment `id`.
  void ReleaseSegmentPageCache(SpillSegmentId id, uint64_t up_to_offset) const;

  [[nodiscard]] StatusOr<uint64_t> SegmentSize(SpillSegmentId id) const;

  // Drops every segment and truncates the backing file cleanly.
  void Clear() noexcept;

  [[nodiscard]] SpillStoreStats Stats() const;
  [[nodiscard]] size_t EffectiveTempLimitBytes() const noexcept;

  // Total blocks handed to platform::AdviseDontNeed since construction.
  // Each completed block is advised at most once per segment lifetime
  // (#1094); tests pin the O(1)-per-block bound on sequential reads.
  [[nodiscard]] uint64_t AdviseDontNeedCalls() const noexcept {
    return advise_calls_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] std::weak_ptr<const bool> LifetimeToken() const noexcept {
    return alive_token_;
  }
  static void DropSegmentIfAlive(SpillStore* store,
                                 const std::weak_ptr<const bool>& token,
                                 SpillSegmentId id) noexcept {
    if (store != nullptr && id != kInvalidSpillSegmentId) {
      if (const auto live = token.lock()) {
        store->DropSegment(id);
      }
    }
  }

 private:
  struct SegmentState {
    std::vector<uint32_t> blocks;
    uint64_t logical_bytes{0};
    bool write_in_progress{false};
    bool pending_drop{false};
    // Number of Read() calls that copied this segment's block numbers into
    // a plan and are still preading outside mu_. DropSegment/Clear/Append
    // must not return these blocks to the free list (or truncate the file)
    // until the count reaches zero, or a reader would see another
    // segment's rewritten bytes. Guarded by mu_; mutable so the const
    // Read path can advance it.
    mutable size_t active_reads{0};
    // Count of blocks already handed to AdviseDontNeed by
    // ReleaseSegmentPageCache; each block is advised at most once per
    // segment lifetime (O(1) amortized syscalls instead of O(B^2) on
    // sequential reads).  Guarded by mu_; mutable so the const release
    // path can advance it.
    mutable size_t advised_blocks{0};
  };

  [[nodiscard]] Status EnsureFileOpenLocked();
  void ReturnBlocksLocked(const std::vector<uint32_t>& blocks) noexcept;
  void MaybeTruncateWhenEmptyLocked() noexcept;

  SpillStoreOptions options_;

  mutable std::mutex mu_;
  int fd_{-1};
  SpillSegmentId next_segment_id_{1};
  uint32_t next_block_index_{0};
  uint64_t active_blocks_{0};
  std::vector<uint32_t> free_blocks_;
  std::unordered_map<SpillSegmentId, SegmentState> segments_;

  std::atomic<uint64_t> bytes_written_{0};
  mutable std::atomic<uint64_t> bytes_read_{0};
  mutable std::atomic<uint64_t> advise_calls_{0};
  uint64_t current_active_bytes_{0};
  uint64_t peak_active_bytes_{0};
  uint64_t physical_file_bytes_{0};
  uint64_t peak_physical_file_bytes_{0};
  uint64_t segments_created_{0};
  uint64_t segments_dropped_{0};
  uint64_t blocks_reused_{0};
  std::shared_ptr<const bool> alive_token_{std::make_shared<const bool>(true)};
};

// Buffered sequential writer onto one `SpillStore` segment.
// Holds at most `buffer_bytes` of staging RAM (configurable so high-fanout
// partitioners remain strictly within small operator budgets). Drops the
// segment automatically on destruction if destroyed before `Finish()`.
class SpillSegmentWriter {
 public:
  explicit SpillSegmentWriter(SpillStore* store,
                              size_t buffer_bytes = SpillStore::kBlockSize);
  ~SpillSegmentWriter();

  SpillSegmentWriter(const SpillSegmentWriter&) = delete;
  SpillSegmentWriter& operator=(const SpillSegmentWriter&) = delete;
  SpillSegmentWriter(SpillSegmentWriter&& other) noexcept;
  SpillSegmentWriter& operator=(SpillSegmentWriter&& other) noexcept;

  // Allocates the underlying logical segment if not yet open.
  [[nodiscard]] Status EnsureOpen();

  // Appends arbitrary bytes; lazily allocates the backing segment on first
  // write if desired.
  [[nodiscard]] Status Write(const void* data, size_t len);
  [[nodiscard]] Status Write(std::string_view sv) {
    return Write(sv.data(), sv.size());
  }

  template <typename T>
  [[nodiscard]] Status WritePod(const T& val) {
    return Write(&val, sizeof(T));
  }

  // Flushes buffered staging bytes to `SpillStore`.
  [[nodiscard]] Status Flush();

  // Flushes remaining bytes and transfers ownership of the completed
  // `SpillSegmentId` to the caller (returns `kInvalidSpillSegmentId` if zero
  // bytes were written unless `EnsureOpen` was explicitly called with
  // the keep-empty option).
  [[nodiscard]] StatusOr<SpillSegmentId> Finish(bool keep_empty = true);

  // Explicit cleanup discarding buffered data and dropping any opened segment.
  void Abort() noexcept;

  [[nodiscard]] SpillSegmentId SegmentId() const noexcept { return id_; }
  [[nodiscard]] uint64_t BytesWritten() const noexcept {
    return bytes_written_;
  }

 private:
  SpillStore* store_{nullptr};
  std::weak_ptr<const bool> store_alive_;
  size_t buffer_capacity_{SpillStore::kBlockSize};
  SpillSegmentId id_{kInvalidSpillSegmentId};
  std::vector<uint8_t> buffer_;
  uint64_t bytes_written_{0};
  bool finished_{false};
};

// Buffered sequential reader over one sealed `SpillStore` segment.
// Uses at most `buffer_bytes` of read-ahead RAM and releases completed kernel
// page-cache blocks progressively.
class SpillSegmentReader {
 public:
  SpillSegmentReader(const SpillStore* store, SpillSegmentId id,
                     size_t buffer_bytes = SpillStore::kBlockSize);

  [[nodiscard]] Status Open();
  [[nodiscard]] Status Seek(uint64_t offset);
  [[nodiscard]] Status ReadExact(void* dst, size_t len);
  [[nodiscard]] StatusOr<size_t> ReadUpTo(void* dst, size_t len);

  template <typename T>
  [[nodiscard]] Status ReadPod(T* out) {
    return ReadExact(out, sizeof(T));
  }

  [[nodiscard]] uint64_t Offset() const noexcept { return cursor_; }
  [[nodiscard]] uint64_t TotalSize() const noexcept { return total_size_; }
  [[nodiscard]] bool Eof() const noexcept {
    return opened_ && cursor_ >= total_size_;
  }
  [[nodiscard]] uint64_t Remaining() const noexcept {
    return cursor_ < total_size_ ? (total_size_ - cursor_) : 0;
  }

 private:
  [[nodiscard]] Status Refill();

  const SpillStore* store_{nullptr};
  SpillSegmentId id_{kInvalidSpillSegmentId};
  size_t buffer_capacity_{SpillStore::kBlockSize};
  uint64_t total_size_{0};
  uint64_t cursor_{0};
  uint64_t buf_start_{0};
  size_t buf_pos_{0};
  size_t buf_len_{0};
  uint64_t last_advised_offset_{0};
  std::vector<uint8_t> buffer_;
  bool opened_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_SPILL_SPILL_STORE_HPP
