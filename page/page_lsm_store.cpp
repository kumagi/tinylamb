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

#include "page/page_lsm_store.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <queue>
#include <utility>

#include "common/crc32c.hpp"
#include "common/exception_status.hpp"
#include "common/memory_budget.hpp"
#include "common/platform/file_io.hpp"

namespace tinylamb {

namespace {

constexpr uint32_t kLsmDataBlockMagic = 0x544C5242U;  // 'TLRB'
constexpr size_t kLsmBlockTargetBytes = 32U << 10;    // 32 KiB
constexpr size_t kMaxDecodedBlockBytes = 64U << 20;
constexpr size_t kWriteBatchBytes = 512U << 10;  // 512 KiB

Status PwriteAllFd(int fd, const void* data, size_t len, uint64_t offset) {
  const auto* ptr = static_cast<const char*>(data);
  size_t done = 0;
  while (done < len) {
    const ssize_t n = platform::Pwrite(fd, ptr + done, len - done,
                                       static_cast<int64_t>(offset + done));
    if (n <= 0) {
      if (n < 0 && errno == EINTR) {
        continue;
      }
      // errno is not guaranteed to be updated when pwrite returns 0
      // (disk full / file-size limit / invalid fd); never print "Success".
      const std::string reason = n == 0
                                     ? "no progress (disk full or invalid fd)"
                                     : std::string(std::strerror(errno));
      return StatusError(StatusCode::kIOError, "LSM pwrite failed: " + reason);
    }
    done += static_cast<size_t>(n);
  }
  return Status::kSuccess;
}

Status PreadAllFd(int fd, void* dst, size_t len, uint64_t offset) {
  auto* ptr = static_cast<char*>(dst);
  size_t done = 0;
  while (done < len) {
    const ssize_t n = platform::Pread(fd, ptr + done, len - done,
                                      static_cast<int64_t>(offset + done));
    if (n == 0) {
      return Status::kCorrupt;
    }
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return StatusError(
          StatusCode::kIOError,
          "LSM pread failed: " + std::string(std::strerror(errno)));
    }
    done += static_cast<size_t>(n);
  }
  return Status::kSuccess;
}

template <typename T>
void AppendLe(std::string* dst, const T& v) {
  dst->append(reinterpret_cast<const char*>(&v), sizeof(T));
}

template <typename T>
bool ConsumeLe(const char* data, size_t size, size_t* off, T* out) {
  if (*off + sizeof(T) > size) {
    return false;
  }
  std::memcpy(out, data + *off, sizeof(T));
  *off += sizeof(T);
  return true;
}

bool ConsumeString(const char* data, size_t size, size_t* off,
                   std::string* out) {
  uint32_t len = 0;
  if (!ConsumeLe(data, size, off, &len) || len > size - *off) {
    return false;
  }
  out->assign(data + *off, len);
  *off += len;
  return true;
}

class LsmRunStreamWriter {
 public:
  LsmRunStreamWriter(int fd, std::mutex& file_mu, uint64_t* append_offset,
                     std::vector<LsmRunBlockMeta>* blocks_out)
      : fd_(fd),
        file_mu_(file_mu),
        append_offset_(append_offset),
        blocks_out_(blocks_out) {
    ResetBlock();
  }

  Status Add(const LsmRunEntry& e) {
    // T2377: both lengths encode as uint32. An entry above 4 GiB would wrap
    // the length field silently -- LoadAndDecodeLsmBlock's ConsumeString
    // then truncates (kCorrupt) or, worse, misaligns every following entry
    // and decodes one key's value as another's. Fail loudly at the primitive
    // boundary instead. (lsm_index.hpp's "no key/value size limits" holds
    // for MemTable intake; the run block format enforces this bound here.)
    if (e.key.size() > std::numeric_limits<uint32_t>::max() ||
        e.value.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::kInvalidArgument;
    }
    // Header (magic/count), entry fields, and trailing CRC all occupy space
    // in the reader's bounded block. Avoid overflow while checking lengths.
    constexpr size_t kBlockOverhead = (2 * sizeof(uint32_t)) + sizeof(uint32_t);
    constexpr size_t kEntryOverhead =
        sizeof(uint64_t) + sizeof(uint8_t) + (2 * sizeof(uint32_t));
    constexpr size_t kMaxPayload =
        kMaxDecodedBlockBytes - kBlockOverhead - kEntryOverhead;
    if (e.key.size() > kMaxPayload ||
        e.value.size() > kMaxPayload - e.key.size()) {
      return Status::kTooBigData;
    }
    const size_t entry_bytes = kEntryOverhead + e.key.size() + e.value.size();
    // Readers use lower_bound on block last_keys; an unsorted producer would
    // silently break Find/Seek. Catch it at the primitive boundary.
    if (has_prev_key_ && e.key < prev_key_) {
      return StatusError(StatusCode::kInvalidArgument,
                         "LsmRunStreamWriter requires sorted keys");
    }
    prev_key_ = e.key;
    has_prev_key_ = true;
    if (block_count_ > 0 &&
        (block_buf_.size() >= kLsmBlockTargetBytes ||
         entry_bytes >
             kMaxDecodedBlockBytes - sizeof(uint32_t) - block_buf_.size())) {
      RETURN_IF_FAIL(FlushCurrentBlock());
    }
    if (e.deleted) {
      has_tombstones_ = true;
    }
    ++total_entries_;
    AppendLe(&block_buf_, static_cast<uint64_t>(e.committed_lsn));
    AppendLe(&block_buf_, static_cast<uint8_t>(e.deleted ? 1U : 0U));
    AppendLe(&block_buf_, static_cast<uint32_t>(e.key.size()));
    block_buf_.append(e.key);
    AppendLe(&block_buf_, static_cast<uint32_t>(e.value.size()));
    block_buf_.append(e.value);
    // Remember where the key bytes live in the block buffer; only the last
    // entry's key is needed, and only when the block is flushed.
    // Layout: lsn(u64) deleted(u8) key_len(u32) key value_len(u32) value, so
    // the key ends right before value_len.
    last_key_offset_ = block_buf_.size() - static_cast<size_t>(e.value.size()) -
                       sizeof(uint32_t) - e.key.size();
    last_key_len_ = e.key.size();
    ++block_count_;
    return Status::kSuccess;
  }

  Status Finish(uint64_t* entry_count_out, bool* has_tombstones_out) {
    if (block_count_ > 0) {
      RETURN_IF_FAIL(FlushCurrentBlock());
    }
    RETURN_IF_FAIL(FlushIoBuffer());
    *entry_count_out = total_entries_;
    *has_tombstones_out = has_tombstones_;
    return Status::kSuccess;
  }

 private:
  void ResetBlock() {
    block_buf_.clear();
    AppendLe(&block_buf_, kLsmDataBlockMagic);
    AppendLe(&block_buf_, uint32_t{0});  // placeholder for entry_count
    block_count_ = 0;
    last_key_offset_ = 0;
    last_key_len_ = 0;
  }

  Status FlushCurrentBlock() {
    if (block_count_ == 0) {
      return Status::kSuccess;
    }
    std::memcpy(block_buf_.data() + sizeof(uint32_t), &block_count_,
                sizeof(block_count_));
    const uint32_t crc = Crc32C(block_buf_.data(), block_buf_.size());
    AppendLe(&block_buf_, crc);
    pending_metas_.push_back(PendingBlock{
        .last_key =
            std::string(block_buf_.data() + last_key_offset_, last_key_len_),
        .rel_offset = io_buf_.size(),
        .length = static_cast<uint32_t>(block_buf_.size()),
        .entry_count = block_count_,
    });
    io_buf_.append(block_buf_);
    ResetBlock();
    if (io_buf_.size() >= kWriteBatchBytes) {
      RETURN_IF_FAIL(FlushIoBuffer());
    }
    return Status::kSuccess;
  }

  Status FlushIoBuffer() {
    if (pending_metas_.empty()) {
      return Status::kSuccess;
    }
    uint64_t write_base = 0;
    {
      std::scoped_lock lk(file_mu_);
      if (fd_ < 0) {
        return Status::kIOError;
      }
      write_base = *append_offset_;
      RETURN_IF_FAIL(
          PwriteAllFd(fd_, io_buf_.data(), io_buf_.size(), write_base));
      *append_offset_ = write_base + io_buf_.size();
    }
    for (PendingBlock& pb : pending_metas_) {
      blocks_out_->push_back(LsmRunBlockMeta{
          .last_key = std::move(pb.last_key),
          .offset = write_base + pb.rel_offset,
          .length = pb.length,
          .entry_count = pb.entry_count,
      });
    }
    io_buf_.clear();
    pending_metas_.clear();
    return Status::kSuccess;
  }

  struct PendingBlock {
    std::string last_key;
    uint64_t rel_offset{0};
    uint32_t length{0};
    uint32_t entry_count{0};
  };

  int fd_;
  std::mutex& file_mu_;
  uint64_t* append_offset_{nullptr};
  std::vector<LsmRunBlockMeta>* blocks_out_{nullptr};
  std::string block_buf_;
  std::string prev_key_;  // last key seen across all blocks (sortedness guard)
  bool has_prev_key_{false};
  size_t last_key_offset_{0};
  size_t last_key_len_{0};
  uint32_t block_count_{0};
  uint64_t total_entries_{0};
  bool has_tombstones_{false};
  std::string io_buf_;
  std::vector<PendingBlock> pending_metas_;
};

StatusOr<std::shared_ptr<const DecodedLsmBlock>> LoadAndDecodeLsmBlock(
    int fd, std::mutex& file_mu, const LsmRunBlockMeta& meta) {
  // `meta.length` comes from an on-disk directory entry. Bound it before the
  // allocation so a corrupt length cannot request gigabytes up front.
  if (meta.length < (3 * sizeof(uint32_t)) ||
      meta.length > kMaxDecodedBlockBytes) {
    return Status::kCorrupt;
  }
  std::string raw(meta.length, '\0');
  {
    std::scoped_lock lk(file_mu);
    if (fd < 0) {
      return Status::kCorrupt;
    }
    RETURN_IF_FAIL(PreadAllFd(fd, raw.data(), meta.length, meta.offset));
  }
  const size_t payload_size = raw.size() - sizeof(uint32_t);
  uint32_t stored_crc = 0;
  std::memcpy(&stored_crc, raw.data() + payload_size, sizeof(uint32_t));
  if (stored_crc != Crc32C(raw.data(), payload_size)) {
    return Status::kCorrupt;
  }
  size_t off = 0;
  uint32_t magic = 0;
  uint32_t count = 0;
  if (!ConsumeLe(raw.data(), payload_size, &off, &magic) ||
      magic != kLsmDataBlockMagic ||
      !ConsumeLe(raw.data(), payload_size, &off, &count) ||
      count != meta.entry_count) {
    return Status::kCorrupt;
  }
  constexpr size_t kMinimumEntryBytes =
      sizeof(uint64_t) + sizeof(uint8_t) + (2 * sizeof(uint32_t));
  if (count > (payload_size - off) / kMinimumEntryBytes) {
    return Status::kCorrupt;
  }
  auto blk = std::make_shared<DecodedLsmBlock>();
  blk->entries.resize(count);
  size_t bytes = sizeof(DecodedLsmBlock) + (count * sizeof(LsmRunEntry));
  for (uint32_t i = 0; i < count; ++i) {
    uint64_t elsn = 0;
    uint8_t del = 0;
    if (!ConsumeLe(raw.data(), payload_size, &off, &elsn) ||
        !ConsumeLe(raw.data(), payload_size, &off, &del) ||
        !ConsumeString(raw.data(), payload_size, &off, &blk->entries[i].key) ||
        !ConsumeString(raw.data(), payload_size, &off,
                       &blk->entries[i].value)) {
      return Status::kCorrupt;
    }
    blk->entries[i].committed_lsn = elsn;
    blk->entries[i].deleted = (del != 0);
    bytes += blk->entries[i].key.size() + blk->entries[i].value.size();
  }
  if (off != payload_size) {
    return Status::kCorrupt;
  }
  blk->approx_bytes = bytes;
  return std::shared_ptr<const DecodedLsmBlock>(std::move(blk));
}

}  // namespace

const EvictedPageDesc* EvictedPageTable::Find(page_id_t pid) const noexcept {
  const page_id_t chunk_id = pid >> kChunkBits;
  const auto it = chunks_.find(chunk_id);
  if (it == chunks_.end() || it->second == nullptr) {
    return nullptr;
  }
  const EvictedPageDesc& slot = it->second->slots[pid & kChunkMask];
  return slot.disk_offset != RowPage::kInvalidDiskOffset ? &slot : nullptr;
}

ssize_t EvictedPageTable::Set(page_id_t pid, const EvictedPageDesc& desc) {
  const page_id_t chunk_id = pid >> kChunkBits;
  const bool invalid = desc.disk_offset == RowPage::kInvalidDiskOffset;
  auto it = chunks_.try_emplace(chunk_id, nullptr).first;
  ssize_t delta = 0;
  if (it->second == nullptr) {
    if (invalid) {
      // Nothing to invalidate and no chunk to hold an invalid descriptor.
      // Allocating one would strand a 4 KiB chunk: the slot stays invalid so
      // Erase returns {false, 0} and never reclaims it until Clear().
      chunks_.erase(it);
      return 0;
    }
    it->second = std::make_unique<Chunk>();
    delta = static_cast<ssize_t>(sizeof(Chunk));
  }
  EvictedPageDesc& slot = it->second->slots[pid & kChunkMask];
  const bool was_valid = slot.disk_offset != RowPage::kInvalidDiskOffset;
  const bool now_valid = desc.disk_offset != RowPage::kInvalidDiskOffset;
  if (!was_valid && now_valid) {
    ++it->second->count;
    ++total_entries_;
  } else if (was_valid && !now_valid) {
    // T4184: invalidating an occupied slot through Set must undo the
    // original insert's bookkeeping; without this, Count() overstates the
    // live entries and Erase's count==0 chunk reclamation never fires,
    // so chunks leak monotonically.
    --it->second->count;
    --total_entries_;
  }
  slot = desc;
  if (was_valid && !now_valid && it->second->count == 0) {
    // Last live slot invalidated: reclaim the now-empty chunk here as well,
    // mirroring Erase's count==0 path, so Bytes()/memory stay accurate.
    chunks_.erase(it);
    delta -= static_cast<ssize_t>(sizeof(Chunk));
  }
  return delta;
}

std::pair<bool, ssize_t> EvictedPageTable::Erase(page_id_t pid) noexcept {
  const page_id_t chunk_id = pid >> kChunkBits;
  const auto it = chunks_.find(chunk_id);
  if (it == chunks_.end() || it->second == nullptr) {
    return {false, 0};
  }
  EvictedPageDesc& slot = it->second->slots[pid & kChunkMask];
  if (slot.disk_offset == RowPage::kInvalidDiskOffset) {
    return {false, 0};
  }
  slot = EvictedPageDesc{};
  if (total_entries_ > 0) {
    --total_entries_;
  }
  if (it->second->count > 0) {
    --it->second->count;
  }
  if (it->second->count == 0) {
    chunks_.erase(it);
    return {true, -static_cast<ssize_t>(sizeof(Chunk))};
  }
  return {true, 0};
}

void EvictedPageTable::NextBatchAfter(
    page_id_t after_pid, size_t limit,
    std::vector<std::pair<page_id_t, EvictedPageDesc>>* out) const {
  out->clear();
  if (limit == 0 || after_pid == std::numeric_limits<page_id_t>::max()) {
    // Only max itself has no successor. Rejecting max-1 here would skip the
    // descriptor at max (start_chunk/first_slot below handle max-1 without
    // overflowing after_pid + 1), silently dropping it from checkpoint and
    // eviction scans.
    return;
  }
  const page_id_t start_chunk = (after_pid + 1) >> kChunkBits;
  for (auto it = chunks_.lower_bound(start_chunk); it != chunks_.end(); ++it) {
    if (it->second == nullptr || it->second->count == 0) {
      continue;
    }
    const page_id_t base = it->first << kChunkBits;
    const size_t first_slot =
        base <= after_pid ? static_cast<size_t>((after_pid - base) + 1) : 0U;
    for (size_t s = first_slot; s < kChunkSize; ++s) {
      const EvictedPageDesc& slot = it->second->slots[s];
      if (slot.disk_offset != RowPage::kInvalidDiskOffset) {
        out->emplace_back(base | static_cast<page_id_t>(s), slot);
        if (out->size() >= limit) {
          return;
        }
      }
    }
  }
}

void EvictedPageTable::Clear() noexcept {
  chunks_.clear();
  total_entries_ = 0;
}

void BoundedFreeIdSet::Insert(page_id_t pid) {
  if (pid == 0) {
    return;
  }
  auto next = by_lo_.upper_bound(pid);
  bool merge_left = false;
  std::map<page_id_t, page_id_t>::iterator prev;
  if (next != by_lo_.begin()) {
    prev = std::prev(next);
    if (prev->second >= pid) {
      return;
    }
    merge_left = (prev->second + 1 == pid);
  }
  const bool merge_right = (next != by_lo_.end() && pid + 1 == next->first);
  if (merge_left && merge_right) {
    prev->second = next->second;
    by_lo_.erase(next);
  } else if (merge_left) {
    prev->second = pid;
  } else if (merge_right) {
    const page_id_t hi = next->second;
    by_lo_.erase(next);
    by_lo_.emplace(pid, hi);
  } else {
    by_lo_.emplace(pid, pid);
  }
  if (by_lo_.size() > kMaxRanges) {
    by_lo_.erase(by_lo_.begin());
  }
}

void BoundedFreeIdSet::Erase(page_id_t pid) {
  auto it = by_lo_.upper_bound(pid);
  if (it == by_lo_.begin()) {
    return;
  }
  --it;
  if (pid > it->second) {
    return;
  }
  const page_id_t lo = it->first;
  const page_id_t hi = it->second;
  if (lo == pid && hi == pid) {
    by_lo_.erase(it);
  } else if (lo == pid) {
    by_lo_.erase(it);
    if (pid == std::numeric_limits<page_id_t>::max()) {
      return;
    }
    by_lo_.emplace(pid + 1, hi);
  } else if (hi == pid) {
    it->second = pid - 1;
  } else {
    it->second = pid - 1;
    if (pid == std::numeric_limits<page_id_t>::max()) {
      return;
    }
    by_lo_.emplace(pid + 1, hi);
  }
  // A middle split grows the range count by one; enforce the same bound as
  // Insert/LoadRanges (drop the lowest range) so a checkpoint snapshot can
  // never exceed kMaxRanges and be rejected as kCorrupt on the next load.
  if (by_lo_.size() > kMaxRanges) {
    by_lo_.erase(by_lo_.begin());
  }
}

bool BoundedFreeIdSet::TryPop(page_id_t* pid_out) noexcept {
  while (!by_lo_.empty()) {
    auto it = std::prev(by_lo_.end());
    if (it->second == 0) {
      // pid 0 is reserved/invalid; Insert and LoadRanges exclude it, but a
      // malformed range can still reach here (e.g. a crafted checkpoint).
      // Drop it instead of handing 0 to AllocateNewPage.
      by_lo_.erase(it);
      continue;
    }
    *pid_out = it->second;
    if (it->first == it->second) {
      by_lo_.erase(it);
    } else {
      --it->second;
    }
    return true;
  }
  return false;
}

bool BoundedFreeIdSet::Contains(page_id_t pid) const noexcept {
  auto it = by_lo_.upper_bound(pid);
  if (it == by_lo_.begin()) {
    return false;
  }
  --it;
  return pid >= it->first && pid <= it->second;
}

void BoundedFreeIdSet::LoadRanges(
    std::span<const std::pair<page_id_t, page_id_t>> ranges) {
  by_lo_.clear();
  for (const auto& [lo, hi] : ranges) {
    if (lo != 0 && lo <= hi) {
      by_lo_.emplace(lo, hi);
      if (by_lo_.size() > kMaxRanges) {
        by_lo_.erase(by_lo_.begin());
      }
    }
  }
}

const LsmRunEntry* DecodedLsmBlock::Find(std::string_view key) const noexcept {
  auto it = std::lower_bound(
      entries.begin(), entries.end(), key,
      [](const LsmRunEntry& e, std::string_view k) { return e.key < k; });
  if (it != entries.end() && it->key == key) {
    return &*it;
  }
  return nullptr;
}

LsmBlockCache::~LsmBlockCache() { Clear(); }

void LsmBlockCache::ReserveStorageBytes(size_t bytes,
                                        size_t* emergency_out) noexcept {
  if (bytes == 0) {
    return;
  }
  if (!ProcessMemoryBudget::Global().TryReserve(MemoryPoolKind::kStorage, bytes,
                                                /*respect_soft_limit=*/false)) {
    // T1835: centrally capped; untracked on rejection (emergency_out is the
    // release-pairing flag, so it only counts granted reserves).
    if (ProcessMemoryBudget::Global().ReserveEmergency(MemoryPoolKind::kStorage,
                                                       bytes)) {
      *emergency_out += bytes;
    }
  }
}

void LsmBlockCache::ReleaseStorageBytes(size_t bytes,
                                        size_t emergency_bytes) noexcept {
  if (bytes == 0) {
    return;
  }
  const size_t em_rel = std::min(bytes, emergency_bytes);
  if (em_rel > 0) {
    ProcessMemoryBudget::Global().ReleaseEmergency(MemoryPoolKind::kStorage,
                                                   em_rel);
  }
  const size_t norm_rel = bytes - em_rel;
  if (norm_rel > 0) {
    ProcessMemoryBudget::Global().Release(MemoryPoolKind::kStorage, norm_rel);
  }
}

StatusOr<std::shared_ptr<const DecodedLsmBlock>> LsmBlockCache::GetOrLoad(
    int fd, std::mutex& file_mu, uint64_t run_id, uint32_t block_idx,
    const LsmRunBlockMeta& meta, size_t limit_bytes, bool populate_cache) try {
  const CacheKey key{.run_id = run_id, .block_idx = block_idx};
  {
    std::scoped_lock lk(mu_);
    if (auto it = map_.find(key); it != map_.end()) {
      lru_.splice(lru_.end(), lru_, it->second.lru_it);
      return it->second.block;
    }
  }
  ASSIGN_OR_RETURN(std::shared_ptr<const DecodedLsmBlock>, loaded,
                   LoadAndDecodeLsmBlock(fd, file_mu, meta));
  if (!populate_cache || limit_bytes == 0 ||
      loaded->approx_bytes > limit_bytes) {
    return loaded;
  }
  const size_t entry_bytes = loaded->approx_bytes;
  std::scoped_lock lk(mu_);
  if (auto it = map_.find(key); it != map_.end()) {
    lru_.splice(lru_.end(), lru_, it->second.lru_it);
    return it->second.block;
  }
  while (cached_bytes_.load(std::memory_order_relaxed) >
             limit_bytes - entry_bytes &&
         !lru_.empty()) {
    const CacheKey victim = lru_.front();
    lru_.pop_front();
    auto vit = map_.find(victim);
    if (vit != map_.end()) {
      const size_t vb = vit->second.bytes;
      map_.erase(vit);
      cached_bytes_.fetch_sub(vb, std::memory_order_relaxed);
      const size_t em_rel = std::min(vb, emergency_bytes_);
      emergency_bytes_ -= em_rel;
      ReleaseStorageBytes(vb, em_rel);
    }
  }
  lru_.push_back(key);
  try {
    const auto [it, inserted] =
        map_.emplace(key, Entry{.block = loaded,
                                .bytes = entry_bytes,
                                .lru_it = std::prev(lru_.end())});
    if (!inserted) {
      lru_.pop_back();
      lru_.splice(lru_.end(), lru_, it->second.lru_it);
      return it->second.block;
    }
  } catch (...) {
    lru_.pop_back();
    throw;
  }
  // Publish accounting only after both cache containers own the entry.
  ReserveStorageBytes(entry_bytes, &emergency_bytes_);
  cached_bytes_.fetch_add(entry_bytes, std::memory_order_relaxed);
  return loaded;
} catch (...) {
  return StatusFromCurrentException("LSM block cache load failed");
}

size_t LsmBlockCache::EvictToTarget(size_t target_bytes,
                                    bool try_lock_only) noexcept {
  std::unique_lock<std::mutex> lk(mu_, std::defer_lock);
  if (try_lock_only) {
    if (!lk.try_lock()) {
      return 0;
    }
  } else {
    lk.lock();
  }
  size_t freed = 0;
  while (cached_bytes_.load(std::memory_order_relaxed) > target_bytes &&
         !lru_.empty()) {
    const CacheKey victim = lru_.front();
    lru_.pop_front();
    auto it = map_.find(victim);
    if (it == map_.end()) {
      continue;
    }
    const size_t vb = it->second.bytes;
    map_.erase(it);
    cached_bytes_.fetch_sub(vb, std::memory_order_relaxed);
    freed += vb;
    const size_t em_rel = std::min(vb, emergency_bytes_);
    emergency_bytes_ -= em_rel;
    ReleaseStorageBytes(vb, em_rel);
  }
  return freed;
}

void LsmBlockCache::InvalidateRun(uint64_t run_id) noexcept {
  std::scoped_lock lk(mu_);
  for (auto it = lru_.begin(); it != lru_.end();) {
    if (it->run_id == run_id) {
      auto mit = map_.find(*it);
      if (mit != map_.end()) {
        const size_t vb = mit->second.bytes;
        map_.erase(mit);
        cached_bytes_.fetch_sub(vb, std::memory_order_relaxed);
        const size_t em_rel = std::min(vb, emergency_bytes_);
        emergency_bytes_ -= em_rel;
        ReleaseStorageBytes(vb, em_rel);
      }
      it = lru_.erase(it);
    } else {
      ++it;
    }
  }
}

void LsmBlockCache::Clear() noexcept {
  std::scoped_lock lk(mu_);
  const size_t total = cached_bytes_.exchange(0, std::memory_order_acq_rel);
  const size_t em = std::exchange(emergency_bytes_, 0);
  map_.clear();
  lru_.clear();
  ReleaseStorageBytes(total, std::min(total, em));
}

LsmRunCursor::LsmRunCursor(int fd, std::mutex& file_mu, LsmBlockCache& cache,
                           uint64_t run_id,
                           std::span<const LsmRunBlockMeta> blocks,
                           size_t cache_limit_bytes, bool populate_cache)
    : fd_(fd),
      file_mu_(file_mu),
      cache_(cache),
      run_id_(run_id),
      blocks_(blocks),
      cache_limit_bytes_(cache_limit_bytes),
      populate_cache_(populate_cache) {}

Status LsmRunCursor::LoadBlock(size_t bidx) {
  if (bidx >= blocks_.size()) {
    block_.reset();
    block_idx_ = blocks_.size();
    entry_idx_ = 0;
    return Status::kSuccess;
  }
  ASSIGN_OR_RETURN(
      std::shared_ptr<const DecodedLsmBlock>, decoded,
      cache_.GetOrLoad(fd_, file_mu_, run_id_, static_cast<uint32_t>(bidx),
                       blocks_[bidx], cache_limit_bytes_, populate_cache_));
  block_ = std::move(decoded);
  block_idx_ = bidx;
  entry_idx_ = 0;
  return Status::kSuccess;
}

Status LsmRunCursor::SeekToFirst() { return LoadBlock(0); }

Status LsmRunCursor::Seek(std::string_view key) {
  auto it = std::lower_bound(blocks_.begin(), blocks_.end(), key,
                             [](const LsmRunBlockMeta& b, std::string_view k) {
                               return b.last_key < k;
                             });
  if (it == blocks_.end()) {
    block_.reset();
    block_idx_ = blocks_.size();
    entry_idx_ = 0;
    return Status::kSuccess;
  }
  const auto bidx = static_cast<size_t>(std::distance(blocks_.begin(), it));
  RETURN_IF_FAIL(LoadBlock(bidx));
  if (block_ == nullptr) {
    return Status::kSuccess;
  }
  auto eit = std::lower_bound(
      block_->entries.begin(), block_->entries.end(), key,
      [](const LsmRunEntry& e, std::string_view k) { return e.key < k; });
  entry_idx_ = static_cast<size_t>(std::distance(block_->entries.begin(), eit));
  if (entry_idx_ >= block_->entries.size()) {
    return LoadBlock(bidx + 1);
  }
  return Status::kSuccess;
}

Status LsmRunCursor::Next() {
  if (block_ == nullptr) {
    return Status::kSuccess;
  }
  ++entry_idx_;
  if (entry_idx_ >= block_->entries.size()) {
    return LoadBlock(block_idx_ + 1);
  }
  return Status::kSuccess;
}

Status WriteLsmRunStream(int fd, std::mutex& file_mu, uint64_t* append_offset,
                         const std::function<bool(LsmRunEntry*)>& next_entry,
                         std::vector<LsmRunBlockMeta>* blocks_out,
                         uint64_t* entry_count_out, bool* has_tombstones_out) {
  // Initialize all outputs up front like MergeLsmSourcesToFd, so an early
  // failure cannot leave stale metadata in caller-owned variables.
  blocks_out->clear();
  *entry_count_out = 0;
  *has_tombstones_out = false;
  LsmRunStreamWriter writer(fd, file_mu, append_offset, blocks_out);
  LsmRunEntry entry;
  while (next_entry(&entry)) {
    RETURN_IF_FAIL(writer.Add(entry));
  }
  return writer.Finish(entry_count_out, has_tombstones_out);
}

Status MergeLsmSourcesToFd(
    int in_fd, std::mutex& in_file_mu, int out_fd, std::mutex& out_file_mu,
    uint64_t* out_append_offset, LsmBlockCache& cache,
    std::span<const LsmRunSpec> runs,
    const std::function<bool(LsmRunEntry*)>& next_mem_entry,
    bool drop_tombstones, std::vector<LsmRunBlockMeta>* blocks_out,
    uint64_t* entry_count_out, bool* has_tombstones_out) {
  blocks_out->clear();
  *entry_count_out = 0;
  *has_tombstones_out = false;

  std::vector<LsmRunCursor> cursors;
  cursors.reserve(runs.size());
  for (const LsmRunSpec& r : runs) {
    cursors.emplace_back(in_fd, in_file_mu, cache, r.run_id, r.blocks, 0,
                         /*populate_cache=*/false);
    RETURN_IF_FAIL(cursors.back().SeekToFirst());
  }

  const size_t mem_source_id = runs.size();
  LsmRunEntry mem_cur;
  bool mem_valid = next_mem_entry ? next_mem_entry(&mem_cur) : false;

  struct HeapItem {
    std::string key;
    size_t source_id{0};
    LsmRunEntry entry;
  };
  // Min-heap by key ascending; on equal keys, HIGHEST source_id (newest) first.
  auto cmp = [](const HeapItem& a, const HeapItem& b) {
    const int c = a.key.compare(b.key);
    if (c != 0) {
      return c > 0;
    }
    return a.source_id < b.source_id;
  };
  std::priority_queue<HeapItem, std::vector<HeapItem>, decltype(cmp)> pq(cmp);

  for (size_t i = 0; i < cursors.size(); ++i) {
    if (cursors[i].Valid()) {
      LsmRunEntry e = cursors[i].Current();
      pq.push(HeapItem{.key = e.key, .source_id = i, .entry = std::move(e)});
    }
  }
  if (mem_valid) {
    pq.push(HeapItem{.key = mem_cur.key,
                     .source_id = mem_source_id,
                     .entry = std::move(mem_cur)});
  }

  LsmRunStreamWriter writer(out_fd, out_file_mu, out_append_offset, blocks_out);

  auto advance_source = [&](size_t src_id) -> Status {
    if (src_id == mem_source_id) {
      mem_valid = next_mem_entry(&mem_cur);
      if (mem_valid) {
        pq.push(HeapItem{.key = mem_cur.key,
                         .source_id = mem_source_id,
                         .entry = std::move(mem_cur)});
      }
      return Status::kSuccess;
    }
    RETURN_IF_FAIL(cursors[src_id].Next());
    if (cursors[src_id].Valid()) {
      LsmRunEntry e = cursors[src_id].Current();
      pq.push(
          HeapItem{.key = e.key, .source_id = src_id, .entry = std::move(e)});
    }
    return Status::kSuccess;
  };

  while (!pq.empty()) {
    HeapItem winner = pq.top();
    pq.pop();
    const std::string cur_key = winner.key;
    // Drain all older duplicates for the same key and advance their cursors.
    while (!pq.empty() && pq.top().key == cur_key) {
      const size_t loser_src = pq.top().source_id;
      pq.pop();
      RETURN_IF_FAIL(advance_source(loser_src));
    }
    RETURN_IF_FAIL(advance_source(winner.source_id));

    if (!drop_tombstones || !winner.entry.deleted) {
      RETURN_IF_FAIL(writer.Add(winner.entry));
    }
  }
  return writer.Finish(entry_count_out, has_tombstones_out);
}

}  // namespace tinylamb
