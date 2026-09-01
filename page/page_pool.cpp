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

#include "page_pool.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <array>
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <ranges>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "meta_page.hpp"
#include "page/page_ref.hpp"
#include "page_type.hpp"
#include "recovery/recovery_manager.hpp"

namespace tinylamb {

namespace {

int OpenPageFile(std::string_view file_name) {
  const int fd = ::open(std::string(file_name).c_str(),
                        O_RDWR | O_CREAT | O_CLOEXEC, 0666);
  if (fd < 0) {
    throw std::runtime_error("failed to open file: " + std::string(file_name) +
                             ": " + std::strerror(errno));
  }
  return fd;
}

#ifdef __APPLE__
int SyncFile(int fd) { return ::fcntl(fd, F_FULLFSYNC); }
#else
int SyncFile(int fd) { return ::fdatasync(fd); }
#endif

void SyncFileChecked(int fd) {
  if (SyncFile(fd) < 0) {
    LOG(ERROR) << "page file sync failed: " << std::strerror(errno);
  }
}

// Transient errors that justify re-issuing pread/pwrite.
bool RetryableErrno(int err) {
  return err == EINTR || err == EAGAIN || err == EWOULDBLOCK;
}

// Computes the file byte offset for `pid`, rejecting ids whose scaled offset
// would overflow off_t into negative territory (EINVAL territory).
bool PageOffset(page_id_t pid, off_t* out) {
  constexpr uint64_t kMaxPid =
      static_cast<uint64_t>(std::numeric_limits<off_t>::max()) / kPageSize;
  if (pid > kMaxPid) {
    return false;
  }
  *out = static_cast<off_t>(static_cast<uint64_t>(pid) * kPageSize);
  return true;
}

}  // namespace

PagePool::PagePool(std::string_view file_name, size_t capacity)
    : file_name_(file_name), fd_(OpenPageFile(file_name)), capacity_(capacity) {}

void PagePool::SetDurabilityGate(std::function<void(lsn_t)> gate) {
  durability_gate_ = std::move(gate);
}

PageRef PagePool::GetPage(page_id_t page_id, bool* cache_hit, bool shared) {
  return GetPageImpl(page_id, cache_hit, shared, true);
}

PageRef PagePool::GetPageForRecovery(page_id_t page_id, bool* cache_hit) {
  return GetPageImpl(page_id, cache_hit, false, false);
}

PageRef PagePool::GetPageImpl(page_id_t page_id, bool* cache_hit, bool shared,
                              bool validate) {
  // The install below releases pool_latch for file I/O, so concurrent misses
  // can refill the pool meanwhile. Retry the miss path a few times so the
  // eviction loop can restore capacity_ before installing; the cap keeps a
  // fully-pinned pool from spinning forever (it then installs over capacity,
  // bounded by the number of racing misses).
  constexpr int kMaxInstallAttempts = 4;
  int attempts = 0;
  for (;;) {
    // Fast hit path: stripe-local map + atomic pin. pool_latch is not
    // touched at all; LRU Touch below stays best-effort.
    Page* hit_page = nullptr;
    std::shared_mutex* hit_page_latch = nullptr;
    std::atomic<uint32_t>* hit_pin_count = nullptr;
    {
      PoolShard& shard = shards_[ShardIndex(page_id)];
      std::scoped_lock shard_latch(shard.mu);
      if (auto entry = shard.map.find(page_id); entry != shard.map.end()) {
        Entry& resident = *entry->second;
        resident.pin_count.fetch_add(1, std::memory_order_relaxed);
        hit_page = resident.page.get();
        hit_page_latch = resident.page_latch.get();
        hit_pin_count = &resident.pin_count;
        if (cache_hit != nullptr) {
          *cache_hit = true;
        }
      }
    }
    if (hit_page != nullptr) {
      // A hit already proved recency. Updating the global LRU list on every
      // B-tree level/heap lookup turns one contended cache line into the OLTP
      // bottleneck while adding almost no eviction information for hot
      // pages. Sample touches; misses and the slow recheck path still touch
      // unconditionally, preserving cold-page admission behavior.
      thread_local uint32_t touch_sample = 0;
      if ((++touch_sample & 63U) == 0) {
        std::unique_lock touch_latch(pool_latch, std::try_to_lock);
        if (touch_latch.owns_lock()) {
          if (auto again = pool_.find(page_id); again != pool_.end()) {
            Touch(again->second);
          }
        }
      }
      return {this, hit_page, hit_page_latch, shared, hit_pin_count};
    }

    std::unique_lock latch(pool_latch);
    // Recheck under exclusive lock: another thread may have installed the
    // page, or eviction bookkeeping may have detached it from its stripe map
    // only (see DetachVictim).
    if (auto entry = pool_.find(page_id); entry != pool_.end()) {
      entry->second->pin_count.fetch_add(1, std::memory_order_relaxed);
      Touch(entry->second);
      const LruType::iterator refreshed = pool_.at(page_id);
      Page* const page = refreshed->page.get();
      std::shared_mutex* const page_latch = refreshed->page_latch.get();
      std::atomic<uint32_t>* const pin_count = &refreshed->pin_count;
      if (cache_hit != nullptr) {
        *cache_hit = true;
      }
      latch.unlock();
      return {this, page, page_latch, shared, pin_count};
    }

    while (pool_lru_.size() >= capacity_) {
      std::unique_ptr<Page> victim;
      if (!DetachVictim(&victim)) {
        break;
      }
      const uint64_t victim_id = victim->PageID();
      flushing_.insert(victim_id);
      latch.unlock();
      try {
        std::scoped_lock io(io_latches_[ShardIndex(victim_id)].mu);
        WriteBack(victim.get());
      } catch (...) {
        latch.lock();
        flushing_.erase(victim_id);
        // Keep the victim resident: dropping it here would silently lose its
        // dirty image. Reattach under pool_latch so a racing miss either pins
        // this entry or waits for it; only an id that raced back into the
        // pool cannot be reattached.
        if (!pool_.contains(victim_id)) {
          pool_lru_.emplace_back(victim.release());
          pool_lru_.back().pin_count.store(0, std::memory_order_relaxed);
          const LruType::iterator restored = std::prev(pool_lru_.end());
          pool_.emplace(victim_id, restored);
          PoolShard& shard = shards_[ShardIndex(victim_id)];
          std::scoped_lock shard_latch(shard.mu);
          shard.map.emplace(victim_id, &*restored);
        } else {
          victim.reset();
        }
        throw;
      }
      victim.reset();
      latch.lock();
      flushing_.erase(victim_id);
    }
    if (pool_.contains(page_id)) {
      continue;
    }

    if (cache_hit != nullptr) {
      *cache_hit = false;
    }

    auto new_page = std::make_unique<Page>(page_id, PageType::kUnknown);
    auto new_page_latch = std::make_unique<std::shared_mutex>();
    latch.unlock();
    // Ordering protocol against concurrent evictions: an evictor registers
    // the victim in flushing_ (under pool_latch) BEFORE taking this page
    // id's IO latch to pwrite it. By checking flushing_ while holding the
    // same IO latch as ReadFrom below, a clear check means any racing
    // write-back of this page id has already finished, or is serialized
    // strictly after our ReadFrom.
    for (;;) {
      bool pending = false;
      {
        std::scoped_lock io(io_latches_[ShardIndex(page_id)].mu);
        std::shared_lock check(pool_latch);
        pending = flushing_.contains(page_id);
        if (!pending) {
          ReadFrom(new_page.get(), page_id, validate);
          break;
        }
      }
      std::this_thread::yield();
    }
    latch.lock();
    if (auto raced = pool_.find(page_id); raced != pool_.end()) {
      raced->second->pin_count.fetch_add(1, std::memory_order_relaxed);
      Touch(raced->second);
      const LruType::iterator refreshed = pool_.at(page_id);
      Page* const page = refreshed->page.get();
      std::shared_mutex* const page_latch = refreshed->page_latch.get();
      std::atomic<uint32_t>* const pin_count = &refreshed->pin_count;
      latch.unlock();
      return {this, page, page_latch, shared, pin_count};
    }

    // A racing eviction may have detached this very page id after our read
    // began: installing our stale on-disk image while that dirty write-back
    // is still in flight would resurrect outdated contents (and surface
    // wrong page types to callers). Never install under a pending flush;
    // discard the load and retry once the write-back completes.
    if (flushing_.contains(page_id)) {
      latch.unlock();
      std::this_thread::yield();
      continue;
    }

    // Page-id verification: only an image that claims this very id may be
    // installed, so a misdirected/torn load can never surface a foreign
    // page (the "invalid page type" corruption class). The recovery path
    // (validate=false) deliberately hands broken images back verbatim.
    if (validate && new_page->PageID() != page_id) {
      latch.unlock();
      LOG(ERROR) << "loaded page image id mismatch: requested=" << page_id
                 << " image=" << new_page->PageID() << " status="
                 << Status::kCorrupt;
      throw std::runtime_error("page id mismatch on load: page_id=" +
                               std::to_string(page_id));
    }

    // Capacity was last validated before the latch was released for I/O; a
    // concurrent miss may have filled the pool meanwhile. Discard this load
    // and retry so the eviction loop runs first.
    if (pool_lru_.size() >= capacity_ && ++attempts < kMaxInstallAttempts) {
      continue;
    }

    Page* const raw_page = new_page.release();
    std::shared_mutex* const raw_latch = new_page_latch.get();
    pool_lru_.emplace_back(raw_page);
    pool_lru_.back().page_latch = std::move(new_page_latch);
    const LruType::iterator installed = std::prev(pool_lru_.end());
    std::atomic<uint32_t>* const installed_pin_count = &installed->pin_count;
    pool_.emplace(page_id, installed);
    {
      PoolShard& shard = shards_[ShardIndex(page_id)];
      std::scoped_lock shard_latch(shard.mu);
      shard.map.emplace(page_id, &*installed);
    }
    latch.unlock();
    // Page content was loaded without holding page_latch, so the caller may
    // take a shared page latch when requested.
    return {this, raw_page, raw_latch, shared, installed_pin_count};
  }
}

void PagePool::DropAllPages() {
  std::unique_lock latch(pool_latch);
  // Everything moves to retired_: pinned entries keep their live PageRefs
  // working as before, and unpinned-but-referenced entries (a stripe-map hit
  // between lookup and pin) can never dangle because their memory outlives
  // the pool. Nothing here is written back, matching the contract.
  retired_.splice(retired_.end(), pool_lru_);
  pool_.clear();
  for (PoolShard& shard : shards_) {
    std::scoped_lock shard_latch(shard.mu);
    shard.map.clear();
  }
}

void PagePool::FlushPageForTest(page_id_t page_id) {
  {
    std::unique_lock latch(pool_latch);
    const auto it = pool_.find(page_id);
    if (it == pool_.end()) {
      return;  // Already evicted.
    }
    // Pin the entry so a concurrent eviction cannot detach and destroy the
    // Page between unlocking pool_latch and WriteBack (the raw pointer would
    // dangle the moment the evictor's unique_ptr resets).
    it->second->pin_count.fetch_add(1, std::memory_order_relaxed);
    Touch(it->second);
  }
  std::scoped_lock io(io_latches_[ShardIndex(page_id)].mu);
  std::unique_lock latch(pool_latch);
  const auto it = pool_.find(page_id);
  if (it == pool_.end()) {
    // Evicted and re-created?  The pin we hold kept the original Page alive
    // through DetachVictim's window, so reaching here means the entry was
    // retired (DropAllPages); just drop our pin.
    latch.unlock();
    return;
  }
  Page* target = it->second->page.get();
  latch.unlock();
  WriteBack(target);
  latch.lock();
  const auto still = pool_.find(page_id);
  if (still != pool_.end()) {
    ReleasePin(*still->second, page_id);
  }
}

void PagePool::Unpin(page_id_t page_id) {
  // Fast path: resolve the entry via its stripe and decrement outside any
  // pool-wide latch so parallel unpins and hit-path pins never serialize
  // behind eviction bookkeeping.
  {
    PoolShard& shard = shards_[ShardIndex(page_id)];
    std::scoped_lock shard_latch(shard.mu);
    if (auto entry = shard.map.find(page_id); entry != shard.map.end()) {
      ReleasePin(*entry->second, page_id);
      return;
    }
  }
  // Slow path: the entry is momentarily absent from its stripe map (mid
  // eviction bookkeeping) or retired by DropAllPages; resolve it through the
  // pool map instead. A pinned entry always remains in pool_ until its pins
  // drop to zero, so a legitimate unpin never misses here silently.
  std::shared_lock latch(pool_latch);
  const auto entry = pool_.find(page_id);
  if (entry == pool_.end()) {
    return;  // Dropped or retired; nothing to release.
  }
  ReleasePin(*entry->second, page_id);
}

// Precondition: no latch requirement; operates on the entry atomically.
void PagePool::ReleasePin(Entry& entry, page_id_t page_id) {
  uint32_t pins = entry.pin_count.load(std::memory_order_relaxed);
  while (pins != 0) {
    if (entry.pin_count.compare_exchange_weak(pins, pins - 1,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
      return;
    }
  }
  LOG(ERROR) << "unpin underflow on page " << page_id;
}

// Precondition: pool_latch is locked exclusively.
bool PagePool::DetachVictim(std::unique_ptr<Page>* victim) {
  assert(!pool_latch.try_lock());
  for (auto target = pool_lru_.begin(); target != pool_lru_.end(); ++target) {
    if (0 < target->pin_count.load(std::memory_order_relaxed)) {
      continue;
    }
    const uint64_t page_id = target->page->PageID();
    PoolShard& shard = shards_[ShardIndex(page_id)];
    bool confirmed = false;
    {
      // Remove from the stripe map first so no new pin can start, then
      // recheck pin count under the same mutex: a racer that already loaded
      // this entry must have completed its fetch_add inside that critical
      // section, so a zero here means nobody can hold or obtain this entry
      // anymore.
      std::scoped_lock shard_latch(shard.mu);
      shard.map.erase(page_id);
      if (0 < target->pin_count.load(std::memory_order_relaxed)) {
        shard.map.emplace(page_id, &*target);  // Still pinned; restore.
      } else {
        confirmed = true;
      }
    }
    if (!confirmed) {
      continue;
    }
    *victim = std::move(target->page);
    pool_lru_.erase(target);
    pool_.erase(page_id);
    return true;
  }
  return false;
}

// Precondition: pool_latch is locked exclusively.
void PagePool::Touch(LruType::iterator it) {
  assert(!pool_latch.try_lock());
  // Splice the node instead of moving the Entry value: list nodes (and with
  // them the Entry addresses published in the stripe maps) stay stable.
  pool_lru_.splice(pool_lru_.end(), pool_lru_, it);
  pool_[it->page->PageID()] = it;
}

PagePool::~PagePool() {
  // Collect dirty pages first so WriteBack's durability gate never runs
  // under pool_latch.
  std::vector<Page*> dirty;
  {
    std::unique_lock latch(pool_latch);
    for (auto& it : pool_lru_) {
      const uint32_t pins = it.pin_count.load(std::memory_order_relaxed);
      if (0 < pins) {
        LOG(ERROR) << "caution: pinned page(" << it.page->PageID()
                   << ") is to be deleted at pin count " << pins;
        // A live PageRef may read/write this image concurrently and its
        // destructor will touch the pool afterwards; SetChecksum+pwrite here
        // would be a data race, so the pinned page is never written back.
        continue;
      }
      if (!it.page->ChecksumMatches()) {
        dirty.push_back(it.page.get());
      }
    }
    retired_.clear();
  }
  try {
    for (Page* page : dirty) {
      std::scoped_lock io(io_latches_[ShardIndex(page->PageID())].mu);
      WriteBack(page);
    }
    if (fd_ >= 0) {
      SyncFileChecked(fd_);
      ::close(fd_);
      fd_ = -1;
    }
  } catch (...) {
    LOG(ERROR) << "page pool destruction lost " << dirty.size()
               << " dirty pages";
  }
}

void PagePool::WriteBack(const Page* target) {
  // WAL rule: before a modified page image reaches the file, every log record
  // up to its page LSN must be durable. The gate may block (fsync), which is
  // why callers must not hold pool_latch here.
  if (durability_gate_ && !target->ChecksumMatches()) {
    durability_gate_(target->PageLSN());
  }
  target->SetChecksum();
  std::array<char, kPageSize> disk_image{};
  target->EncodeDisk(disk_image.data());
  off_t offset = 0;
  if (!PageOffset(target->PageID(), &offset)) {
    throw std::runtime_error("page offset out of range: page_id=" +
                             std::to_string(target->PageID()));
  }
  const char* buffer = disk_image.data();
  size_t remaining = kPageSize;
  while (remaining > 0) {
    const ssize_t written = ::pwrite(fd_, buffer, remaining, offset);
    if (written < 0) {
      if (RetryableErrno(errno)) {
        continue;
      }
      throw std::runtime_error("cannot write back page " +
                               std::to_string(target->PageID()) + ": " +
                               std::strerror(errno));
    }
    buffer += written;
    offset += written;
    remaining -= static_cast<size_t>(written);
  }
}

// Precondition: target has allocated memory at kPageSize.
void PagePool::ReadFrom(Page* target, page_id_t pid, bool validate) const {
  off_t offset = 0;
  if (!PageOffset(pid, &offset)) {
    LOG(ERROR) << "page offset out of range: page_id=" << pid
               << " status=" << Status::kCorrupt;
    throw std::runtime_error("page offset out of range: page_id=" +
                             std::to_string(pid));
  }
  std::array<char, kPageSize> disk_image{};
  ssize_t nread = 0;
  do {
    nread = ::pread(fd_, disk_image.data(), kPageSize, offset);
  } while (nread < 0 && RetryableErrno(errno));
  if (nread < 0) {
    // EINVAL (offset overflow) and friends are hard failures, never a
    // silent empty-page materialization.
    LOG(ERROR) << "cannot read page " << pid << ": " << std::strerror(errno);
    throw std::runtime_error("cannot read page " + std::to_string(pid) + ": " +
                             std::strerror(errno));
  }
  if (nread == 0) {
    // Past EOF: a cleanly never-written region; materialize a free page in
    // memory (checksum stays 0 until WriteBack).
    target->PageInit(pid, PageType::kFreePage);
  } else if (std::cmp_less(nread, kPageSize)) {
    // Partial image: unlike a clean EOF this smells like a torn write. Keep
    // the free-page materialization (callers depend on it) but say so loudly
    // instead of silently discarding real bytes.
    LOG(ERROR) << "short read on page " << pid << ": " << nread << "/"
               << kPageSize << " bytes; possible torn write, treating the "
               << "image as a fresh free page";
    target->PageInit(pid, PageType::kFreePage);
  } else {
    const bool all_zero = std::ranges::all_of(
        disk_image, [](char byte) { return byte == 0; });
    if (all_zero) {
      target->PageInit(pid, PageType::kFreePage);
      nread = 0;  // Fresh sparse region has no checksum to validate.
    } else {
      try {
        target->DecodeDisk(disk_image.data());
      } catch (const std::runtime_error& error) {
        // Preserve a recoverable, checksum-invalid placeholder. Recovery
        // opens pages without validation and reconstructs this image from
        // WAL; ordinary readers still hit the validation failure below.
        LOG(ERROR) << "invalid page format on page_id=" << pid << ": "
                   << error.what();
        target->PageInit(pid, PageType::kUnknown);
        target->format_magic = 0;
        target->checksum = 0;
      }
    }
  }
  if (nread == kPageSize && !target->IsValid()) {
    if (target->type == PageType::kUnknown && target->checksum == 0) {
      // A freshly extended file region reads as zeros; uninitialized rather
      // than corrupt. Real pages always carry a non-zero CRC after WriteBack.
      target->PageInit(pid, PageType::kFreePage);
    } else if (validate) {
      LOG(ERROR) << "corrupt page checksum on page_id=" << pid
                 << " status=" << Status::kCorrupt;
      throw std::runtime_error("corrupt page checksum: page_id=" +
                               std::to_string(pid));
    }
    // Otherwise hand the broken image back verbatim: the recovery manager
    // detects it via IsValid() and rebuilds the page from the log.
  }

  // RecLSN = MAX means a clean page.
  target->recovery_lsn = std::numeric_limits<lsn_t>::max();
}

}  // namespace tinylamb
