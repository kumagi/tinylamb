/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_PAGE_LSM_STORE_HPP
#define TINYLAMB_PAGE_LSM_STORE_HPP

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/row_page.hpp"

namespace tinylamb {

// Compact 16-byte descriptor for a non-resident committed RowPage whose payload
// resides in `<db>.blocks.<generation>`.
struct EvictedPageDesc {
  static constexpr uint64_t kUncommittedOnlyDiskOffset =
      RowPage::kInvalidDiskOffset - 1;

  uint64_t disk_offset{RowPage::kInvalidDiskOffset};
  uint32_t disk_length{0};
  uint32_t file_gen{0};

  [[nodiscard]] bool HasCommittedDiskBlock() const noexcept {
    return disk_offset != RowPage::kInvalidDiskOffset &&
           disk_offset != kUncommittedOnlyDiskOffset;
  }
};
static_assert(sizeof(EvictedPageDesc) == 16);

// Two-level sparse chunk table for non-resident page descriptors (~16 B/page
// dense, zero per-page heap nodes or mutexes, O(1) lookup, ordered cursor
// scan).
class EvictedPageTable {
 public:
  static constexpr page_id_t kChunkBits = 8;
  static constexpr page_id_t kChunkSize = page_id_t{1} << kChunkBits;  // 256
  static constexpr page_id_t kChunkMask = kChunkSize - 1;

  struct Chunk {
    uint16_t count{0};
    std::array<EvictedPageDesc, kChunkSize> slots{};
  };

  EvictedPageTable() = default;

  [[nodiscard]] const EvictedPageDesc* Find(page_id_t pid) const noexcept;
  [[nodiscard]] bool Contains(page_id_t pid) const noexcept {
    return Find(pid) != nullptr;
  }

  // Returns signed delta to `Bytes()` (`+ssize_t(sizeof(Chunk))` on new chunk).
  ssize_t Set(page_id_t pid, const EvictedPageDesc& desc);

  // Returns `{erased, signed delta to Bytes()}`.
  std::pair<bool, ssize_t> Erase(page_id_t pid) noexcept;

  // Fills up to `limit` entries strictly after `after_pid` in ascending order.
  void NextBatchAfter(
      page_id_t after_pid, size_t limit,
      std::vector<std::pair<page_id_t, EvictedPageDesc>>* out) const;

  void Clear() noexcept;

  [[nodiscard]] size_t Count() const noexcept { return total_entries_; }
  [[nodiscard]] size_t Bytes() const noexcept {
    return chunks_.size() * sizeof(Chunk);
  }
  [[nodiscard]] bool Empty() const noexcept { return total_entries_ == 0; }

 private:
  std::map<page_id_t, std::unique_ptr<Chunk>> chunks_;
  size_t total_entries_{0};
};

// Bounded, range-coalesced set of reusable page IDs (ADR 0009).
class BoundedFreeIdSet {
 public:
  static constexpr size_t kMaxRanges = 65536;

  void Insert(page_id_t pid);
  void Erase(page_id_t pid);
  bool TryPop(page_id_t* pid_out) noexcept;
  [[nodiscard]] bool Contains(page_id_t pid) const noexcept;
  void Clear() noexcept { by_lo_.clear(); }

  void LoadRanges(std::span<const std::pair<page_id_t, page_id_t>> ranges);
  [[nodiscard]] const std::map<page_id_t, page_id_t>& Ranges() const noexcept {
    return by_lo_;
  }
  [[nodiscard]] size_t RangeCount() const noexcept { return by_lo_.size(); }
  [[nodiscard]] bool Empty() const noexcept { return by_lo_.empty(); }

 private:
  std::map<page_id_t, page_id_t> by_lo_;
};

struct LsmRunEntry {
  std::string key;
  std::string value;
  bool deleted{false};
  lsn_t committed_lsn{0};
};

struct LsmRunBlockMeta {
  std::string last_key;
  uint64_t offset{0};
  uint32_t length{0};
  uint32_t entry_count{0};
};

struct DecodedLsmBlock {
  std::vector<LsmRunEntry> entries;
  size_t approx_bytes{0};

  [[nodiscard]] const LsmRunEntry* Find(std::string_view key) const noexcept;
};

struct LsmRunSpec {
  uint64_t run_id{0};
  std::span<const LsmRunBlockMeta> blocks;
};

// Byte-bounded LRU cache for decoded SSTable blocks under
// MemoryPoolKind::kStorage.
class LsmBlockCache {
 public:
  LsmBlockCache() = default;
  ~LsmBlockCache();

  LsmBlockCache(const LsmBlockCache&) = delete;
  LsmBlockCache& operator=(const LsmBlockCache&) = delete;
  LsmBlockCache(LsmBlockCache&&) = delete;
  LsmBlockCache& operator=(LsmBlockCache&&) = delete;

  [[nodiscard]] StatusOr<std::shared_ptr<const DecodedLsmBlock>> GetOrLoad(
      int fd, std::mutex& file_mu, uint64_t run_id, uint32_t block_idx,
      const LsmRunBlockMeta& meta, size_t limit_bytes, bool populate_cache);

  size_t EvictToTarget(size_t target_bytes,
                       bool try_lock_only = false) noexcept;
  void InvalidateRun(uint64_t run_id) noexcept;
  void Clear() noexcept;

  [[nodiscard]] size_t CachedBytes() const noexcept {
    return cached_bytes_.load(std::memory_order_relaxed);
  }

 private:
  struct CacheKey {
    uint64_t run_id{0};
    uint32_t block_idx{0};
    bool operator==(const CacheKey& o) const noexcept {
      return run_id == o.run_id && block_idx == o.block_idx;
    }
  };
  struct CacheKeyHash {
    size_t operator()(const CacheKey& k) const noexcept {
      uint64_t h = k.run_id * 0x9E3779B97F4A7C15ULL;
      h ^= static_cast<uint64_t>(k.block_idx) + 0x9E3779B97F4A7C15ULL +
           (h << 6) + (h >> 2);
      return static_cast<size_t>(h);
    }
  };
  struct Entry {
    std::shared_ptr<const DecodedLsmBlock> block;
    size_t bytes{0};
    std::list<CacheKey>::iterator lru_it;
  };

  static void ReserveStorageBytes(size_t bytes, size_t* emergency_out) noexcept;
  static void ReleaseStorageBytes(size_t bytes,
                                  size_t emergency_bytes) noexcept;

  mutable std::mutex mu_;
  std::list<CacheKey> lru_;
  std::unordered_map<CacheKey, Entry, CacheKeyHash> map_;
  std::atomic<size_t> cached_bytes_{0};
  size_t emergency_bytes_{0};
};

// Streaming cursor over one disk-backed SSTable run (~32 KiB of encoded block
// bytes per cursor (plus decoded rows)).
class LsmRunCursor {
 public:
  LsmRunCursor(int fd, std::mutex& file_mu, LsmBlockCache& cache,
               uint64_t run_id, std::span<const LsmRunBlockMeta> blocks,
               size_t cache_limit_bytes, bool populate_cache);

  Status SeekToFirst();
  Status Seek(std::string_view key);
  Status Next();

  [[nodiscard]] bool Valid() const noexcept {
    return block_ != nullptr && entry_idx_ < block_->entries.size();
  }
  [[nodiscard]] const LsmRunEntry& Current() const noexcept {
    // T4094: defensive for consumers that skip Valid(): an exhausted cursor
    // hands back a stable empty entry instead of dereferencing a reset
    // block_ or indexing past the last decoded entry.
    static const LsmRunEntry kInvalidEntry{};
    if (block_ == nullptr || entry_idx_ >= block_->entries.size()) {
      return kInvalidEntry;
    }
    return block_->entries[entry_idx_];
  }

 private:
  Status LoadBlock(size_t bidx);

  int fd_;
  std::mutex& file_mu_;
  LsmBlockCache& cache_;
  uint64_t run_id_{0};
  std::span<const LsmRunBlockMeta> blocks_;
  size_t cache_limit_bytes_{0};
  bool populate_cache_{false};
  size_t block_idx_{0};
  size_t entry_idx_{0};
  std::shared_ptr<const DecodedLsmBlock> block_;
};

// Writes sorted entries from an arbitrary producer callback as CRC-protected
// ~32 KiB SSTable blocks sequentially into `fd` starting at `*append_offset`.
Status WriteLsmRunStream(int fd, std::mutex& file_mu,
                         uint64_t* append_offset,
                         const std::function<bool(LsmRunEntry*)>& next_entry,
                         std::vector<LsmRunBlockMeta>* blocks_out,
                         uint64_t* entry_count_out, bool* has_tombstones_out);

// Streaming K-way heap merge across `runs` (ordered oldest -> newest) plus an
// optional newest sorted memory overlay producer (`next_mem_entry`), writing
// the merged output run to `out_fd` (~O(K * 32 KiB) memory total).
Status MergeLsmSourcesToFd(
    int in_fd, std::mutex& in_file_mu, int out_fd,
    std::mutex& out_file_mu, uint64_t* out_append_offset, LsmBlockCache& cache,
    std::span<const LsmRunSpec> runs,
    const std::function<bool(LsmRunEntry*)>& next_mem_entry,
    bool drop_tombstones, std::vector<LsmRunBlockMeta>* blocks_out,
    uint64_t* entry_count_out, bool* has_tombstones_out);

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_LSM_STORE_HPP
