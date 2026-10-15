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
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <ranges>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
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

StatusOr<int> OpenPageFile(std::string_view file_name) {
  const int fd = ::open(std::string(file_name).c_str(),
                        O_RDWR | O_CREAT | O_CLOEXEC, 0666);
  if (fd < 0) {
    return StatusError(StatusCode::kIOError,
                       "failed to open file: " + std::string(file_name) + ": " +
                           std::strerror(errno));
  }
  return fd;
}

#ifdef __APPLE__
int SyncFile(int fd) {
  int rc = 0;
  do {
    rc = ::fcntl(fd, F_FULLFSYNC);
  } while (rc < 0 && errno == EINTR);
  return rc;
}
#else
int SyncFile(int fd) {
  int rc = 0;
  do {
    rc = ::fdatasync(fd);
  } while (rc < 0 && errno == EINTR);
  return rc;
}
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

StatusOr<std::unique_ptr<PagePool>> PagePool::Create(std::string_view file_name,
                                                     size_t capacity) {
  ASSIGN_OR_RETURN(int, fd, OpenPageFile(file_name));
  int lock_result = 0;
  do {
    lock_result = ::flock(fd, LOCK_EX | LOCK_NB);
  } while (lock_result != 0 && errno == EINTR);
  if (lock_result != 0) {
    const int lock_errno = errno;
    ::close(fd);
    if (lock_errno == EWOULDBLOCK || lock_errno == EAGAIN) {
      return StatusError(StatusCode::kConflicts,
                         "page file is already open by another PagePool: " +
                             std::string(file_name));
    }
    return StatusError(StatusCode::kIOError, "failed to lock page file " +
                                                 std::string(file_name) + ": " +
                                                 std::strerror(lock_errno));
  }
  // The fd is owned by this frame until the pool takes it: a throwing `new`
  // (bad_alloc) must close it here or the descriptor leaks (same pattern as
  // Logger::Create).
  std::unique_ptr<PagePool> pool;
  try {
    // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
    pool = std::unique_ptr<PagePool>(
        new PagePool(file_name, capacity, fd));  // NOLINT
  } catch (const std::bad_alloc&) {
    ::close(fd);
    return StatusError(
        StatusCode::kIOError,
        "failed to allocate page pool for " + std::string(file_name));
  } catch (...) {
    ::close(fd);
    throw;
  }
  return pool;
}

PagePool::PagePool(std::string_view file_name, size_t capacity, int fd)
    : file_name_(file_name), fd_(fd), capacity_(capacity) {}

void PagePool::SetDurabilityGate(std::function<Status(lsn_t)> gate) {
  durability_gate_ = std::move(gate);
}

uint64_t PagePool::DebugDataFileSizeForDiag() const {
  struct stat st{};
  if (::fstat(fd_, &st) != 0) {
    return 0;
  }
  return static_cast<uint64_t>(st.st_size);
}

bool PagePool::ContainsResident(page_id_t page_id) const {
  PoolShard& shard = const_cast<PagePool*>(this)->shards_[ShardIndex(page_id)];
  std::scoped_lock shard_latch(shard.mu);
  return shard.map.contains(page_id);
}

StatusOr<PageRef> PagePool::GetPage(page_id_t page_id, bool* cache_hit,
                                    bool shared) {
  return GetPageImpl(page_id, cache_hit, shared, true);
}

StatusOr<PageRef> PagePool::GetPageForRecovery(page_id_t page_id,
                                               bool* cache_hit) {
  StatusOr<PageRef> page = GetPageImpl(page_id, cache_hit, false, false);
  if (page.HasValue() && !page.Value().IsNull() &&
      page.Value()->PageID() != page_id) {
    Page& raw = *page.Value();
    raw.PageInit(page_id, PageType::kUnknown);
    raw.format_magic = 0;
    raw.checksum = 1;
  }
  return page;
}

StatusOr<PageRef> PagePool::GetPageImpl(page_id_t page_id, bool* cache_hit,
                                        bool shared, bool validate) {
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
      cache_hits_.fetch_add(1, std::memory_order_relaxed);
      return LatchPinned(this, hit_page, hit_page_latch, shared, hit_pin_count);
    }

    std::unique_lock latch(pool_latch);
    if (dropping_pages_.load(std::memory_order_acquire)) {
      return StatusError(StatusCode::kRuntimeError,
                         "page pool is dropping resident pages");
    }
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
      cache_hits_.fetch_add(1, std::memory_order_relaxed);
      latch.unlock();
      return LatchPinned(this, page, page_latch, shared, pin_count);
    }

    while (pool_lru_.size() >= capacity_) {
      std::unique_ptr<Page> victim;
      if (!DetachVictim(&victim)) {
        break;
      }
      const uint64_t victim_id = victim->PageID();
      if (victim->ChecksumMatches()) {
        victim.reset();
        continue;
      }
      active_evictions_.fetch_add(1, std::memory_order_acq_rel);
      struct ActiveEvictionGuard {
        std::atomic<uint32_t>& count;
        std::condition_variable_any& cv;
        ActiveEvictionGuard(std::atomic<uint32_t>& count_,
                            std::condition_variable_any& cv_)
            : count(count_), cv(cv_) {}
        ActiveEvictionGuard(const ActiveEvictionGuard&) = delete;
        ActiveEvictionGuard& operator=(const ActiveEvictionGuard&) = delete;
        ActiveEvictionGuard(ActiveEvictionGuard&&) = delete;
        ActiveEvictionGuard& operator=(ActiveEvictionGuard&&) = delete;
        ~ActiveEvictionGuard() {
          if (count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            cv.notify_all();
          }
        }
      } eviction_guard{active_evictions_, writeback_cv_};
      flushing_.insert(victim_id);
      latch.unlock();
      Status write_back = Status::kSuccess;
      {
        std::scoped_lock io(io_latches_[ShardIndex(victim_id)].mu);
        write_back = WriteBack(victim.get());
      }
      if (write_back != Status::kSuccess) {
        latch.lock();
        flushing_.erase(victim_id);
        // Keep the victim resident: dropping it here would silently lose its
        // dirty image. Reattach under pool_latch so a racing miss either
        // pins this entry or waits for it; only an id that raced back into
        // the pool cannot be reattached. Keep victim owning the image until
        // the node is constructed (std::bad_alloc from Entry's latch
        // allocation or the list node is the only remaining throw here).
        if (!pool_.contains(victim_id)) {
          Page* const raw_victim = victim.get();
          try {
            pool_lru_.emplace_back(raw_victim, victim_id);
          } catch (const std::bad_alloc& error) {
            LOG(ERROR) << "cannot reattach unwritten victim page " << victim_id
                       << ": " << error.what();
          }
          if (!pool_lru_.empty() && pool_lru_.back().page.get() == raw_victim) {
            // Release only after the node owns the image: the raw pointer
            // is stored in the list, so the released value is redundant.
            victim.release();  // NOLINT(bugprone-unused-return-value)
            pool_lru_.back().pin_count.store(0, std::memory_order_relaxed);
            const auto restored = std::prev(pool_lru_.end());
            pool_.emplace(victim_id, restored);
            PoolShard& shard = shards_[ShardIndex(victim_id)];
            std::scoped_lock shard_latch(shard.mu);
            shard.map.emplace(victim_id, &*restored);
          } else {
            victim.reset();
          }
          latch.unlock();
          return write_back;
        }
        victim.reset();
        latch.unlock();
        return write_back;
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
    active_loads_.fetch_add(1, std::memory_order_acq_rel);
    struct ActiveLoadGuard {
      std::atomic<uint32_t>& count;
      std::condition_variable_any& cv;
      ActiveLoadGuard(std::atomic<uint32_t>& count_,
                      std::condition_variable_any& cv_)
          : count(count_), cv(cv_) {}
      ActiveLoadGuard(const ActiveLoadGuard&) = delete;
      ActiveLoadGuard& operator=(const ActiveLoadGuard&) = delete;
      ActiveLoadGuard(ActiveLoadGuard&&) = delete;
      ActiveLoadGuard& operator=(ActiveLoadGuard&&) = delete;
      ~ActiveLoadGuard() {
        if (count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
          cv.notify_all();
        }
      }
    } load_guard{active_loads_, writeback_cv_};
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
          RETURN_IF_FAIL(ReadFrom(new_page.get(), page_id, validate));
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
      cache_hits_.fetch_add(1, std::memory_order_relaxed);
      latch.unlock();
      return LatchPinned(this, page, page_latch, shared, pin_count);
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
                 << " image=" << new_page->PageID()
                 << " status=" << Status::kCorrupt;
      return StatusError(
          StatusCode::kCorrupt,
          "page id mismatch on load: page_id=" + std::to_string(page_id));
    }

    // Capacity was last validated before the latch was released for I/O; a
    // concurrent miss may have filled the pool meanwhile. Discard this load
    // and retry so the eviction loop runs first.
    if (pool_lru_.size() >= capacity_ && ++attempts < kMaxInstallAttempts) {
      continue;
    }

    // Exception safety: keep new_page owning the image until the list node
    // is fully constructed. Entry::Entry allocates its page_latch and the
    // list allocates its node -- either can throw, and releasing ownership
    // first would leak the image.
    Page* const raw_page = new_page.get();
    std::shared_mutex* const raw_latch = new_page_latch.get();
    try {
      pool_lru_.emplace_back(raw_page, page_id);
    } catch (const std::bad_alloc& error) {
      // new_page still owns raw_page; nothing installed. std::bad_alloc
      // from the list node is a memory-exhaustion condition, not a DB
      // logic error: report it like any other GetPage failure.
      latch.unlock();
      return StatusError(
          StatusCode::kRuntimeError,
          std::string("cannot allocate pool entry: ") + error.what());
    }
    // The list node now owns the image; the released pointer is already
    // stored in the node, so discarding it is intentional.
    new_page.release();  // NOLINT(bugprone-unused-return-value)
    pool_lru_.back().page_latch = std::move(new_page_latch);
    const auto installed = std::prev(pool_lru_.end());
    try {
      pool_.emplace(page_id, installed);
      {
        PoolShard& shard = shards_[ShardIndex(page_id)];
        std::scoped_lock shard_latch(shard.mu);
        shard.map.emplace(page_id, &*installed);
      }
    } catch (const std::bad_alloc& error) {
      // Roll back the half-installed node.  pool_.emplace may have landed
      // before the shard insert threw; leaving its entry would hand a
      // dangling list iterator to the next miss-path recheck (and block
      // re-installation forever), so undo whichever maps saw it.  The
      // exclusive pool latch is still held, so erase(page_id) can only
      // remove the entry emplace just created.
      pool_.erase(page_id);
      pool_lru_.erase(installed);
      latch.unlock();
      return StatusError(
          StatusCode::kRuntimeError,
          std::string("cannot register pool entry: ") + error.what());
    }
    std::atomic<uint32_t>* const installed_pin_count = &installed->pin_count;
    latch.unlock();
    // Page content was loaded without holding page_latch, so the caller may
    // take a shared page latch when requested.
    cache_misses_.fetch_add(1, std::memory_order_relaxed);
    return LatchPinned(this, raw_page, raw_latch, shared, installed_pin_count);
  }
}

StatusOr<PageRef> PagePool::LatchPinned(PagePool* pool, Page* page,
                                        std::shared_mutex* page_latch,
                                        bool shared,
                                        std::atomic<uint32_t>* pin_count) {
  try {
    return PageRef(pool, page, page_latch, shared, pin_count);
  } catch (const std::system_error& error) {
    // The pin was taken before the latch attempt, so unwind it here: leaving
    // it would pin the entry forever, and ~PagePool then skips its write-back
    // ("pinned page is to be deleted").
    pin_count->fetch_sub(1, std::memory_order_acq_rel);
    return StatusError(StatusCode::kRuntimeError,
                       std::string("failed to latch page ") +
                           std::to_string(page->PageID()) + ": " +
                           error.what());
  }
}

void PagePool::DropAllPages() {
  {
    // Flip the flag under the exclusive latch so every concurrent
    // WriteBack/SyncDataFile either registered its active count before this
    // critical section (and is awaited below) or observes the flag and
    // refuses. Without this ordering a registration could slip past the
    // count wait and write a page image after the caller assumed the pool
    // was quiescent.
    std::unique_lock flip(pool_latch);
    dropping_pages_.store(true, std::memory_order_release);
  }
  {
    std::unique_lock wait_lock(writeback_wait_mu_);
    while (active_writebacks_.load(std::memory_order_acquire) != 0 ||
           active_evictions_.load(std::memory_order_acquire) != 0 ||
           active_loads_.load(std::memory_order_acquire) != 0) {
      writeback_cv_.wait_for(wait_lock, std::chrono::milliseconds(10));
    }
  }
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
  dropping_pages_.store(false, std::memory_order_release);
  latch.unlock();
  writeback_cv_.notify_all();
}

Status PagePool::FlushPage(page_id_t page_id) {
  for (;;) {
    Entry* pinned = nullptr;
    {
      std::unique_lock latch(pool_latch);
      if (dropping_pages_.load(std::memory_order_acquire)) {
        return StatusError(StatusCode::kRuntimeError,
                           "page pool is dropping resident pages");
      }
      const auto it = pool_.find(page_id);
      if (it != pool_.end()) {
        pinned = &*it->second;
        pinned->pin_count.fetch_add(1, std::memory_order_relaxed);
        Touch(it->second);
      } else if (!flushing_.contains(page_id)) {
        return Status::kSuccess;
      }
    }
    if (pinned == nullptr) {
      std::this_thread::yield();
      continue;
    }
    struct UnpinGuard {
      Entry* entry;
      page_id_t id;
      UnpinGuard(Entry* entry_, page_id_t id_) : entry(entry_), id(id_) {}
      UnpinGuard(const UnpinGuard&) = delete;
      UnpinGuard& operator=(const UnpinGuard&) = delete;
      UnpinGuard(UnpinGuard&&) = delete;
      UnpinGuard& operator=(UnpinGuard&&) = delete;
      ~UnpinGuard() { PagePool::ReleasePin(*entry, id); }
    } guard{pinned, page_id};
    std::unique_lock page_latch(*pinned->page_latch);
    if (pinned->page->ChecksumMatches() &&
        pinned->page->RecoveryLSN() == std::numeric_limits<lsn_t>::max()) {
      return Status::kSuccess;
    }
    std::scoped_lock io(io_latches_[ShardIndex(page_id)].mu);
    return WriteBack(pinned->page.get(), pinned);
  }
}

Status PagePool::FlushPageForTest(page_id_t page_id) {
  // Hold the Entry address (not a map iterator): list-node addresses stay
  // stable across Touch splices and DropAllPages retires, and a pinned entry
  // can never be detached, so this pointer outlives every path below.  The
  // guard releases the pin even when WriteBack throws.
  Entry* pinned = nullptr;
  // True when the pin we take below is the only one: no PageRef exists
  // anywhere, so the page latch is free and we can exclude mutators while
  // WriteBack encodes the image.
  bool exclusive_pin = false;
  {
    std::unique_lock latch(pool_latch);
    if (dropping_pages_.load(std::memory_order_acquire)) {
      return StatusError(StatusCode::kRuntimeError,
                         "page pool is dropping resident pages");
    }
    const auto it = pool_.find(page_id);
    if (it == pool_.end()) {
      return Status::kSuccess;  // Already evicted.
    }
    // Pin the entry so a concurrent eviction cannot detach and destroy the
    // Page between unlocking pool_latch and WriteBack (the raw pointer would
    // dangle the moment the evictor's unique_ptr resets).
    pinned = &*it->second;
    exclusive_pin =
        pinned->pin_count.fetch_add(1, std::memory_order_relaxed) == 0;
    Touch(it->second);
  }
  struct UnpinGuard {
    Entry* entry;
    page_id_t id;
    UnpinGuard(Entry* entry_, page_id_t id_) : entry(entry_), id(id_) {}
    UnpinGuard(const UnpinGuard&) = delete;
    UnpinGuard& operator=(const UnpinGuard&) = delete;
    UnpinGuard(UnpinGuard&&) = delete;
    UnpinGuard& operator=(UnpinGuard&&) = delete;
    ~UnpinGuard() { PagePool::ReleasePin(*entry, id); }
  } guard{pinned, page_id};
  // WriteBack reads the page image; a concurrent PageRef holder mutating it
  // mid-encode could persist a torn snapshot. When this pin is exclusive,
  // take the page latch before the IO latch. Callers that already hold a
  // PageRef hold the page latch first as well; reversing this order would
  // invert against a GetPage miss issued while that caller holds its page.
  if (exclusive_pin) {
    std::unique_lock page_latch(*pinned->page_latch);
    std::scoped_lock io(io_latches_[ShardIndex(page_id)].mu);
    return WriteBack(pinned->page.get(), pinned);
  }
  std::scoped_lock io(io_latches_[ShardIndex(page_id)].mu);
  return WriteBack(pinned->page.get(), pinned);
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
    // Cheap first look (relaxed); the authoritative ACQUIRE recheck happens
    // under the stripe mutex below.
    if (0 < target->pin_count.load(std::memory_order_relaxed)) {
      continue;
    }
    const uint64_t page_id = target->page_id;
    PoolShard& shard = shards_[ShardIndex(page_id)];
    bool confirmed = false;
    {
      // Remove from the stripe map first so no new pin can start, then
      // recheck pin count under the same mutex: a racer that already loaded
      // this entry must have completed its fetch_add inside that critical
      // section, so a zero here means nobody can hold or obtain this entry
      // anymore.
      // TSAN fix: the recheck must be an ACQUIRE load.  PageRef::PageUnlock
      // decrements with a RELEASE fetch_sub; an acquire here establishes
      // happens-before with that decrement, so destroying the Entry (which
      // frees the pin_count memory) cannot race the decrement itself.  The
      // relaxed loads below reported "data race in PagePool::Entry::~Entry".
      std::scoped_lock shard_latch(shard.mu);
      shard.map.erase(page_id);
      if (0 < target->pin_count.load(std::memory_order_acquire)) {
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
  pool_[it->page_id] = it;
}

size_t PagePool::PinnedPageCount() const noexcept {
  std::shared_lock latch(pool_latch);
  size_t count = 0;
  for (const auto& entry : pool_lru_) {
    if (entry.pin_count.load(std::memory_order_relaxed) > 0) {
      ++count;
    }
  }
  for (const auto& entry : retired_) {
    if (entry.pin_count.load(std::memory_order_relaxed) > 0) {
      ++count;
    }
  }
  return count;
}

PagePool::~PagePool() {
  {
    std::unique_lock wait_lock(writeback_wait_mu_);
    while (active_writebacks_.load(std::memory_order_acquire) != 0 ||
           active_evictions_.load(std::memory_order_acquire) != 0 ||
           active_loads_.load(std::memory_order_acquire) != 0) {
      writeback_cv_.wait_for(wait_lock, std::chrono::milliseconds(10));
    }
  }
  // Collect dirty pages first so WriteBack's durability gate never runs
  // under pool_latch.  The id travels with the entry copy: a page that
  // looked unpinned can be re-pinned by a fast-path holder before the
  // write-back below, and reading Page::PageID() then would race the
  // holder's re-initialization of the image.
  std::vector<std::pair<uint64_t, Page*>> dirty;
  {
    std::unique_lock latch(pool_latch);
    for (auto& it : pool_lru_) {
      const uint32_t pins = it.pin_count.load(std::memory_order_relaxed);
      if (0 < pins) {
        LOG(ERROR) << "caution: pinned page(" << it.page_id
                   << ") is to be deleted at pin count " << pins;
        // A live PageRef may read/write this image concurrently and its
        // destructor will touch the pool afterwards; SetChecksum+pwrite here
        // would be a data race, so the pinned page is never written back.
        continue;
      }
      if (!it.page->ChecksumMatches()) {
        dirty.emplace_back(it.page_id, it.page.get());
      }
    }
    // Warn on pinned retired entries exactly like the live ones above: a
    // PageRef still holding a retired page is a contract violation at this
    // boundary; destruction proceeds as for the live list.
    for (const auto& it : retired_) {
      if (it.pin_count.load(std::memory_order_relaxed) != 0) {
        LOG(ERROR) << "caution: pinned retired page(" << it.page_id
                   << ") is to be deleted with the pool";
      }
    }
    retired_.clear();
  }
  size_t lost = 0;
  for (auto& [dirty_id, page] : dirty) {
    std::scoped_lock io(io_latches_[ShardIndex(dirty_id)].mu);
    if (WriteBack(page) != Status::kSuccess) {
      ++lost;
    }
  }
  if (fd_ >= 0) {
    SyncFileChecked(fd_);
  }
  if (lost != 0) {
    LOG(ERROR) << "page pool destruction lost " << lost << " dirty pages";
  }
  // Close the descriptor even when write-back failed: leaking it turns a
  // recoverable shutdown error into a permanent fd leak, and this is the
  // last owner of fd_.
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

void PagePool::SetWalSnapshotSink(
    std::function<StatusOr<lsn_t>(page_id_t, PageType, std::string_view)>
        sink) {
  wal_snapshot_sink_ = std::move(sink);
}

void PagePool::SetSnapshotRetentionFloor(lsn_t floor) {
  std::scoped_lock sync_guard(sync_state_mu_);
  lsn_t current = snapshot_retention_floor_.load(std::memory_order_relaxed);
  while (current < floor && !snapshot_retention_floor_.compare_exchange_weak(
                                current, floor, std::memory_order_release,
                                std::memory_order_relaxed)) {
  }
  if (floor != std::numeric_limits<lsn_t>::max()) {
    std::scoped_lock lock(snapshot_mu_);
    std::erase_if(wal_snapshot_lsns_,
                  [floor](const auto& entry) { return entry.second <= floor; });
  }
}

bool PagePool::SyncDataFile(lsn_t watermark) {
  {
    // Shared registration, symmetric with WriteBack: DropAllPages takes
    // pool_latch exclusively to flip dropping_pages_, so an in-flight sync
    // is always awaited and a post-flip sync is always refused.
    std::shared_lock pool_guard(pool_latch);
    if (dropping_pages_.load(std::memory_order_acquire)) {
      return false;
    }
    active_writebacks_.fetch_add(1, std::memory_order_acq_rel);
  }
  struct ActiveSyncGuard {
    std::atomic<uint32_t>& count;
    std::condition_variable_any& cv;
    ActiveSyncGuard(std::atomic<uint32_t>& count_,
                    std::condition_variable_any& cv_)
        : count(count_), cv(cv_) {}
    ActiveSyncGuard(const ActiveSyncGuard&) = delete;
    ActiveSyncGuard& operator=(const ActiveSyncGuard&) = delete;
    ActiveSyncGuard(ActiveSyncGuard&&) = delete;
    ActiveSyncGuard& operator=(ActiveSyncGuard&&) = delete;
    ~ActiveSyncGuard() {
      if (count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        cv.notify_all();
      }
    }
  } active_guard{active_writebacks_, writeback_cv_};
  if (sync_data_file_fault_for_test_.load(std::memory_order_acquire)) {
    return false;
  }
  uint64_t completed_cutoff = 0;
  {
    std::scoped_lock sync_guard(sync_state_mu_);
    completed_cutoff = next_pwrite_serial_ - 1;
  }
  if (SyncFile(fd_) != 0) {
    return false;
  }
  std::scoped_lock sync_guard(sync_state_mu_);
  for (auto it = unsynced_pages_.begin(); it != unsynced_pages_.end();) {
    const PendingFullPageImage& pending = it->second;
    const bool completed_before_sync =
        pending.completed_pwrite_serial != 0 &&
        pending.completed_pwrite_serial <= completed_cutoff;
    if (!completed_before_sync || pending.pwrite_in_flight ||
        pending.emitting_snapshot || pending.snapshot_end > watermark) {
      ++it;
      continue;
    }
    FloorTrackRemoveLocked(pending.snapshot_end, pending.emitting_snapshot);
    it = unsynced_pages_.erase(it);
  }
  RefreshUnsyncedSnapshotFloorLocked();
  sync_watermark_.store(
      std::max(sync_watermark_.load(std::memory_order_relaxed), watermark),
      std::memory_order_release);
  return true;
}

void PagePool::FloorTrackInsertLocked(lsn_t snapshot_end,
                                      bool emitting_snapshot) {
  if (emitting_snapshot) {
    ++emitting_fpw_count_;
  } else if (snapshot_end != 0) {
    unsynced_snapshot_ends_.insert(snapshot_end);
  }
}

void PagePool::FloorTrackRemoveLocked(lsn_t snapshot_end,
                                      bool emitting_snapshot) {
  if (emitting_snapshot) {
    --emitting_fpw_count_;
  } else if (snapshot_end != 0) {
    const auto found = unsynced_snapshot_ends_.find(snapshot_end);
    if (found != unsynced_snapshot_ends_.end()) {
      unsynced_snapshot_ends_.erase(found);
    }
  }
}

void PagePool::VerifyFloorTrackingLocked() const {
#ifndef NDEBUG
  size_t emitting = 0;
  std::multiset<lsn_t> ends;
  for (const auto& entry : unsynced_pages_) {
    const PendingFullPageImage& pending = entry.second;
    if (pending.emitting_snapshot) {
      ++emitting;
    } else if (pending.snapshot_end != 0) {
      ends.insert(pending.snapshot_end);
    }
  }
  CHECK_MSG(emitting == emitting_fpw_count_,
            "emitting FPW count drifted from unsynced_pages_");
  CHECK_MSG(ends == unsynced_snapshot_ends_,
            "incremental snapshot floor tracking drifted from unsynced_pages_");
#endif
}

void PagePool::RefreshUnsyncedSnapshotFloorLocked() {
  lsn_t floor = 0;
  if (emitting_fpw_count_ != 0) {
    floor = 0;
  } else if (unsynced_snapshot_ends_.empty()) {
    floor = std::numeric_limits<lsn_t>::max();
  } else {
    // LSN UNIT: the checkpoint feeds this floor straight into
    // Logger::RecycleUpTo, which drops a segment once
    // (index + 1) * segment_size <= floor. `snapshot_end` is the EXCLUSIVE
    // end of the kFullPageImage record, so using it as the floor lets a
    // record that ends exactly on a segment boundary be unlinked together
    // with that segment while its data-file pwrite is still unsynced --
    // exactly the image a torn page would need. Report the record's last
    // byte instead, which keeps the whole segment alive.
    floor = *unsynced_snapshot_ends_.begin() - 1;
  }
  unsynced_snapshot_floor_.store(floor, std::memory_order_release);
}

StatusOr<lsn_t> PagePool::EmitSnapshotIfNeeded(const Page* target) {
  if (!wal_snapshot_sink_ || target->ChecksumMatches()) {
    return 0;
  }
  const page_id_t pid = target->PageID();
  lsn_t retained = 0;
  {
    std::scoped_lock lock(snapshot_mu_);
    const auto found = wal_snapshot_lsns_.find(pid);
    if (found != wal_snapshot_lsns_.end()) {
      retained = found->second;
    }
  }
  const lsn_t floor = snapshot_retention_floor_.load(std::memory_order_acquire);
  const lsn_t watermark = sync_watermark_.load(std::memory_order_acquire);
  const lsn_t protect_upto = std::max(floor, watermark);
  if (retained > protect_upto) {
    return 0;
  }

  bool had_previous = false;
  bool previous_emitting = false;
  lsn_t previous_snapshot_end = 0;
  {
    std::scoped_lock sync_guard(sync_state_mu_);
    auto [it, inserted] = unsynced_pages_.try_emplace(pid);
    had_previous = !inserted;
    if (had_previous) {
      previous_snapshot_end = it->second.snapshot_end;
      previous_emitting = it->second.emitting_snapshot;
      FloorTrackRemoveLocked(previous_snapshot_end, previous_emitting);
    }
    it->second.snapshot_end = 0;
    it->second.completed_pwrite_serial = 0;
    it->second.pwrite_in_flight = true;
    it->second.emitting_snapshot = true;
    FloorTrackInsertLocked(0, true);
    RefreshUnsyncedSnapshotFloorLocked();
  }

  const std::string body(reinterpret_cast<const char*>(&target->body),
                         kPageBodySize);
  StatusOr<lsn_t> emitted = wal_snapshot_sink_(pid, target->Type(), body);
  if (!emitted.HasValue()) {
    std::scoped_lock sync_guard(sync_state_mu_);
    auto it = unsynced_pages_.find(pid);
    if (it != unsynced_pages_.end()) {
      FloorTrackRemoveLocked(0, true);
      if (had_previous) {
        it->second.snapshot_end = previous_snapshot_end;
        it->second.completed_pwrite_serial = 0;
        it->second.pwrite_in_flight = true;
        it->second.emitting_snapshot = false;
        FloorTrackInsertLocked(previous_snapshot_end, false);
      } else {
        unsynced_pages_.erase(it);
      }
      RefreshUnsyncedSnapshotFloorLocked();
    }
    return emitted.GetStatus();
  }

  const lsn_t snapshot_end = emitted.Value();
  std::scoped_lock sync_guard(sync_state_mu_);
  if (snapshot_end == 0) {
    FloorTrackRemoveLocked(0, true);
    if (had_previous) {
      auto it = unsynced_pages_.find(pid);
      it->second.snapshot_end = previous_snapshot_end;
      it->second.completed_pwrite_serial = 0;
      it->second.pwrite_in_flight = true;
      it->second.emitting_snapshot = false;
      FloorTrackInsertLocked(previous_snapshot_end, false);
    } else {
      unsynced_pages_.erase(pid);
    }
    RefreshUnsyncedSnapshotFloorLocked();
    return 0;
  }
  {
    std::scoped_lock snapshot_guard(snapshot_mu_);
    wal_snapshot_lsns_[pid] = snapshot_end;
  }
  auto& pending = unsynced_pages_[pid];
  pending.snapshot_end = snapshot_end;
  pending.completed_pwrite_serial = 0;
  pending.pwrite_in_flight = true;
  pending.emitting_snapshot = false;
  FloorTrackRemoveLocked(0, true);
  FloorTrackInsertLocked(snapshot_end, false);
  RefreshUnsyncedSnapshotFloorLocked();
  VerifyFloorTrackingLocked();
  return snapshot_end;
}

// Checkpoint-flush write-back; see the header comment for the latch
// contract.  Phase A snapshots the content under the exclusive page latch
// (FPW emit + image encode from a shadow copy), Phase B runs the durability
// gate and the pwrite with no page latch held (only the IO latch, the same
// lock the latched path holds across I/O), and Phase C re-latches to seal
// the page only when the staged image is still its newest content.  The
// live page's checksum is never touched until the seal, so concurrent
// dirty-checks (another flusher, the checkpoint's DPT snapshot) cannot
// mistake an in-flight staged write for a persisted image.
Status PagePool::WriteBackStaged(page_id_t pid) {
  Entry* entry = nullptr;
  {
    std::shared_lock pool_guard(pool_latch);
    if (dropping_pages_.load(std::memory_order_acquire)) {
      return StatusError(StatusCode::kRuntimeError,
                         "page pool is dropping resident pages");
    }
    const auto found = pool_.find(pid);
    if (found == pool_.end()) {
      // Not resident: an eviction or DropAllPages already dealt with the
      // image (eviction writes back under the victim's latch).
      return Status::kSuccess;
    }
    entry = &*found->second;
    entry->pin_count.fetch_add(1, std::memory_order_relaxed);
    active_writebacks_.fetch_add(1, std::memory_order_acq_rel);
  }
  struct ReleaseGuard {
    Entry* entry;
    page_id_t id;
    std::atomic<uint32_t>* active;
    std::condition_variable_any* cv;
    ReleaseGuard(Entry* entry_, page_id_t id_, std::atomic<uint32_t>* active_,
                 std::condition_variable_any* cv_)
        : entry(entry_), id(id_), active(active_), cv(cv_) {}
    ReleaseGuard(const ReleaseGuard&) = delete;
    ReleaseGuard& operator=(const ReleaseGuard&) = delete;
    ReleaseGuard(ReleaseGuard&&) = delete;
    ReleaseGuard& operator=(ReleaseGuard&&) = delete;
    ~ReleaseGuard() {
      PagePool::ReleasePin(*entry, id);
      if (active->fetch_sub(1, std::memory_order_acq_rel) == 1) {
        cv->notify_all();
      }
    }
  } guard{entry, pid, &active_writebacks_, &writeback_cv_};

  // ---- Phase A: content snapshot under the exclusive page latch ----
  std::array<char, kPageSize> disk_image{};
  lsn_t gate_lsn = 0;
  lsn_t staged_page_lsn = 0;
  {
    std::unique_lock page_latch(*entry->page_latch);
    Page* page = entry->page.get();
    if (page->ChecksumMatches() &&
        page->RecoveryLSN() == std::numeric_limits<lsn_t>::max()) {
      return Status::kSuccess;  // Clean; nothing to persist.
    }
    StatusOr<lsn_t> emitted = EmitSnapshotIfNeeded(page);
    if (!emitted.HasValue()) {
      // EmitSnapshotIfNeeded rolled its own tracking back; mirror the
      // latched path's fail_write state.
      std::scoped_lock lock(sync_state_mu_);
      if (const auto it = unsynced_pages_.find(pid);
          it != unsynced_pages_.end()) {
        it->second.completed_pwrite_serial = 0;
        it->second.pwrite_in_flight = false;
      }
      return emitted.GetStatus();
    }
    gate_lsn = std::max(page->PageLSN(), emitted.Value());
    // Encode from a shadow copy so the live checksum field stays untouched
    // until the seal.
    std::array<char, kPageSize> shadow{};
    std::memcpy(shadow.data(), page, kPageSize);
    Page* snapshot = reinterpret_cast<Page*>(shadow.data());
    snapshot->SetChecksum();
    snapshot->EncodeDisk(disk_image.data());
    staged_page_lsn = page->PageLSN();
    std::scoped_lock lock(sync_state_mu_);
    if (const auto it = unsynced_pages_.find(pid);
        it != unsynced_pages_.end()) {
      it->second.completed_pwrite_serial = 0;
      it->second.pwrite_in_flight = true;
    }
  }

  // ---- Phase B: durability gate + pwrite, page latch NOT held ----
  Status write_status = Status::kSuccess;
  bool wrote_image = false;
  {
    std::scoped_lock io(io_latches_[ShardIndex(pid)].mu);
    // Disk-serial order: another flusher may have completed an image at or
    // above our staged page_lsn while our copy was in flight.  The IO latch
    // guarantees that whoever set that counter also finished its pwrite AND
    // its sync-state bookkeeping, so overwriting the newer image with ours
    // (and re-marking its bookkeeping) would be both wrong and redundant.
    if (entry->written_page_lsn.load(std::memory_order_acquire) >=
        staged_page_lsn) {
      std::scoped_lock lock(sync_state_mu_);
      if (const auto it = unsynced_pages_.find(pid);
          it != unsynced_pages_.end()) {
        it->second.completed_pwrite_serial = next_pwrite_serial_++;
        it->second.pwrite_in_flight = false;
      }
      return Status::kSuccess;
    }
    if (durability_gate_) {
      write_status = durability_gate_(gate_lsn);
    }
    if (write_status == Status::kSuccess &&
        pwrite_fault_for_test_.load(std::memory_order_acquire)) {
      write_status =
          StatusError(StatusCode::kIOError, "injected pwrite failure");
    }
    off_t offset = 0;
    if (write_status == Status::kSuccess && !PageOffset(pid, &offset)) {
      write_status = StatusError(
          StatusCode::kIOError,
          "page offset out of range: page_id=" + std::to_string(pid));
    }
    const char* buffer = disk_image.data();
    size_t remaining = kPageSize;
    while (remaining > 0 && write_status == Status::kSuccess) {
      const ssize_t written = ::pwrite(fd_, buffer, remaining, offset);
      if (written < 0) {
        if (RetryableErrno(errno)) {
          continue;
        }
        write_status =
            StatusError(StatusCode::kIOError, "cannot write back page " +
                                                  std::to_string(pid) + ": " +
                                                  std::strerror(errno));
        break;
      }
      if (written == 0) {
        write_status =
            StatusError(StatusCode::kIOError, "cannot write back page " +
                                                  std::to_string(pid) +
                                                  ": zero-byte write");
        break;
      }
      buffer += written;
      offset += written;
      remaining -= static_cast<size_t>(written);
    }
    if (write_status == Status::kSuccess) {
      pwrite_count_.fetch_add(1, std::memory_order_relaxed);
      wrote_image = true;
    }
    std::scoped_lock lock(sync_state_mu_);
    if (const auto it = unsynced_pages_.find(pid);
        it != unsynced_pages_.end()) {
      if (write_status == Status::kSuccess) {
        it->second.completed_pwrite_serial = next_pwrite_serial_++;
      }
      it->second.pwrite_in_flight = false;
    }
    if (wrote_image) {
      lsn_t known = entry->written_page_lsn.load(std::memory_order_relaxed);
      while (known < staged_page_lsn &&
             !entry->written_page_lsn.compare_exchange_weak(
                 known, staged_page_lsn, std::memory_order_release,
                 std::memory_order_relaxed)) {
      }
    }
  }
  if (write_status != Status::kSuccess) {
    // The live page was never sealed; its dirty markers stand and the next
    // flush rewrites the image.  Fixing the sync state above is all the
    // rollback needed.
    return write_status;
  }

  // ---- Phase C: anchor the page's dirty accounting to the staged image ----
  {
    std::unique_lock page_latch(*entry->page_latch);
    Page* page = entry->page.get();
    if (page->PageLSN() == staged_page_lsn) {
      // Content unchanged since the snapshot: the on-disk image IS this
      // page's current state, so sealing (checksum + clean recLSN) is
      // truthful.
      page->SetChecksum();
      page->ResetRecLSN();
    } else {
      // Re-dirtied while the staged image was in flight.  The on-disk image
      // covers every record up to the staged page_lsn, so the truthful
      // dirty anchor is exactly that LSN.  SetRecLSN is sticky-min and the
      // page may still carry a recLSN from BEFORE the flush (mutations only
      // ever lower it) -- leaving that stale value in place would pin the
      // checkpoint's recycle floor to a record the flush just made
      // redundant, forever.  Reset first, then anchor at the staged LSN;
      // subsequent mutations keep min()-ing down from there until the next
      // flush re-anchors.  The checksum is NOT sealed: memory and disk
      // differ, and the page's next write-back owns the seal.
      page->ResetRecLSN();
      page->SetRecLSN(staged_page_lsn);
    }
  }
  return Status::kSuccess;
}

Status PagePool::WriteBack(Page* target, Entry* entry) {
  {
    // Shared registration against DropAllPages: the exclusive flip in
    // DropAllPages orders itself against this critical section, so no
    // write-back can register after the flip was observed while a completed
    // one went unseen. Concurrent write-backs (checkpoint flush, evictions)
    // hold the latch in shared mode and no longer serialize on it.
    std::shared_lock pool_guard(pool_latch);
    if (dropping_pages_.load(std::memory_order_acquire)) {
      return StatusError(StatusCode::kRuntimeError,
                         "page pool is dropping resident pages");
    }
    active_writebacks_.fetch_add(1, std::memory_order_acq_rel);
  }
  struct ActiveWritebackGuard {
    std::atomic<uint32_t>& count;
    std::condition_variable_any& cv;
    ActiveWritebackGuard(std::atomic<uint32_t>& count_,
                         std::condition_variable_any& cv_)
        : count(count_), cv(cv_) {}
    ActiveWritebackGuard(const ActiveWritebackGuard&) = delete;
    ActiveWritebackGuard& operator=(const ActiveWritebackGuard&) = delete;
    ActiveWritebackGuard(ActiveWritebackGuard&&) = delete;
    ActiveWritebackGuard& operator=(ActiveWritebackGuard&&) = delete;
    ~ActiveWritebackGuard() {
      if (count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        cv.notify_all();
      }
    }
  } active_guard{active_writebacks_, writeback_cv_};
  const page_id_t pid = target->PageID();
  {
    std::scoped_lock lock(sync_state_mu_);
    if (const auto it = unsynced_pages_.find(pid);
        it != unsynced_pages_.end()) {
      it->second.completed_pwrite_serial = 0;
      it->second.pwrite_in_flight = true;
    }
  }
  const auto fail_write = [&](Status status) {
    std::scoped_lock lock(sync_state_mu_);
    if (const auto it = unsynced_pages_.find(pid);
        it != unsynced_pages_.end()) {
      it->second.completed_pwrite_serial = 0;
      it->second.pwrite_in_flight = false;
    }
    return status;
  };

  StatusOr<lsn_t> emitted = EmitSnapshotIfNeeded(target);
  if (!emitted.HasValue()) {
    return fail_write(emitted.GetStatus());
  }
  const lsn_t snapshot_end = emitted.Value();
  if (durability_gate_ && !target->ChecksumMatches()) {
    const lsn_t gate_lsn = std::max(target->PageLSN(), snapshot_end);
    const Status gate = durability_gate_(gate_lsn);
    if (gate != Status::kSuccess) {
      return fail_write(gate);
    }
  }
  off_t offset = 0;
  if (!PageOffset(target->PageID(), &offset)) {
    return fail_write(StatusError(StatusCode::kIOError,
                                  "page offset out of range: page_id=" +
                                      std::to_string(target->PageID())));
  }
  if (pwrite_fault_for_test_.load(std::memory_order_acquire)) {
    return fail_write(
        StatusError(StatusCode::kIOError, "injected pwrite failure"));
  }
  const uint64_t old_checksum = target->checksum;
  target->SetChecksum();
  std::array<char, kPageSize> disk_image{};
  target->EncodeDisk(disk_image.data());
  const auto abort_write = [&](Status status) {
    target->checksum = old_checksum;
    return fail_write(std::move(status));
  };
  const char* buffer = disk_image.data();
  size_t remaining = kPageSize;
  while (remaining > 0) {
    const ssize_t written = ::pwrite(fd_, buffer, remaining, offset);
    if (written < 0) {
      if (RetryableErrno(errno)) {
        continue;
      }
      return abort_write(StatusError(
          StatusCode::kIOError, "cannot write back page " +
                                    std::to_string(target->PageID()) + ": " +
                                    std::strerror(errno)));
    }
    if (written == 0) {
      return abort_write(StatusError(StatusCode::kIOError,
                                     "cannot write back page " +
                                         std::to_string(target->PageID()) +
                                         ": zero-byte write"));
    }
    buffer += written;
    offset += written;
    remaining -= static_cast<size_t>(written);
  }
  pwrite_count_.fetch_add(1, std::memory_order_relaxed);
  if (entry != nullptr) {
    lsn_t known = entry->written_page_lsn.load(std::memory_order_relaxed);
    const lsn_t image_lsn = target->PageLSN();
    while (known < image_lsn && !entry->written_page_lsn.compare_exchange_weak(
                                    known, image_lsn, std::memory_order_release,
                                    std::memory_order_relaxed)) {
    }
  }
  target->ResetRecLSN();
  {
    std::scoped_lock lock(sync_state_mu_);
    if (const auto it = unsynced_pages_.find(pid);
        it != unsynced_pages_.end()) {
      it->second.completed_pwrite_serial = next_pwrite_serial_++;
      it->second.pwrite_in_flight = false;
    }
  }
  return Status::kSuccess;
}

// Precondition: target has allocated memory at kPageSize.
Status PagePool::ReadFrom(Page* target, page_id_t pid, bool validate) const {
  off_t offset = 0;
  if (!PageOffset(pid, &offset)) {
    LOG(ERROR) << "page offset out of range: page_id=" << pid
               << " status=" << Status::kCorrupt;
    return StatusError(
        StatusCode::kCorrupt,
        "page offset out of range: page_id=" + std::to_string(pid));
  }
  static const bool fast_read_disabled =
      std::getenv("TINYLAMB_DISABLE_FAST_PAGE_READ") != nullptr;
  ssize_t nread = 0;
  if (!fast_read_disabled) {
    // Single pread straight into the page image: the buffered path paid one
    // extra full body copy per page (DecodeDisk's 32 KiB memcpy), which sat
    // next to the syscall itself in scan profiles.  The classification below
    // mirrors the buffered path decision for decision; DecodeDiskInPlace
    // reproduces the exact in-memory image DecodeDisk would have produced,
    // so the shared tail (IsValid / recovery_lsn reset) behaves identically.
    // A/B hatch: TINYLAMB_DISABLE_FAST_PAGE_READ=1 restores the buffer path.
    do {
      nread = ::pread(fd_, reinterpret_cast<char*>(target), kPageSize, offset);
    } while (nread < 0 && RetryableErrno(errno));
    if (nread < 0) {
      LOG(ERROR) << "cannot read page " << pid << ": " << std::strerror(errno);
      return StatusError(StatusCode::kIOError, "cannot read page " +
                                                   std::to_string(pid) + ": " +
                                                   std::strerror(errno));
    }
    if (nread == 0) {
      target->PageInit(pid, PageType::kFreePage);
    } else if (std::cmp_less(nread, kPageSize)) {
      LOG(ERROR) << "short read on page " << pid << ": " << nread << "/"
                 << kPageSize << " bytes; possible torn write";
      if (std::cmp_greater_equal(nread, kPageHeaderSize)) {
        // The in-place header fixup reads the whole leading header, so the
        // unwritten tail must be defined memory.
        std::memset(reinterpret_cast<char*>(target) + nread, 0,
                    kPageSize - static_cast<size_t>(nread));
        const Status decode = target->DecodeDiskInPlace();
        if (decode != Status::kSuccess) {
          target->PageInit(pid, PageType::kUnknown);
          target->format_magic = 0;
        }
        target->checksum = 1;
      } else {
        target->PageInit(pid, PageType::kUnknown);
        target->format_magic = 0;
        target->checksum = 1;
      }
      if (validate) {
        return StatusError(StatusCode::kCorrupt,
                           "short page image: page_id=" + std::to_string(pid));
      }
    } else {
      const char* const image = reinterpret_cast<const char*>(target);
      // Cheap pre-check: a page whose header region holds any non-zero byte
      // cannot be an all-zero fresh region.  Only the (rare) zero-header
      // case pays the full page scan.
      const bool header_has_nonzero = std::any_of(
          image, image + kPageHeaderSize, [](char byte) { return byte != 0; });
      bool all_zero = false;
      if (header_has_nonzero) {
        all_zero = false;
      } else {
        all_zero = std::all_of(image, image + kPageSize,
                               [](char byte) { return byte == 0; });
      }
      if (all_zero) {
        target->PageInit(pid, PageType::kFreePage);
        nread = 0;  // Fresh sparse region has no checksum to validate.
      } else {
        const Status decode = target->DecodeDiskInPlace();
        if (decode != Status::kSuccess) {
          // Same recoverable, checksum-invalid placeholder contract as the
          // buffered path: recovery rebuilds the image from WAL; the
          // sentinel checksum keeps the free-page materialization below
          // from silently reclaiming live data.
          LOG(ERROR) << "invalid page format on page_id=" << pid << ": "
                     << decode;
          target->PageInit(pid, PageType::kUnknown);
          target->format_magic = 0;
          target->checksum = 1;
        }
      }
    }
  } else {
    // Uninitialized on purpose: a full read overwrites every byte, and only
    // the short-read path below needs the zeroed tail. The per-page 32 KiB
    // value-init showed up next to the CRC in scan profiles.
    // A/B hatch: TINYLAMB_DISABLE_READBUF_NOINIT=1 restores the value-init.
    static const bool noinit_disabled =
        std::getenv("TINYLAMB_DISABLE_READBUF_NOINIT") != nullptr;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<char, kPageSize> disk_image;
    if (noinit_disabled) {
      disk_image.fill(0);
    }
    do {
      nread = ::pread(fd_, disk_image.data(), kPageSize, offset);
    } while (nread < 0 && RetryableErrno(errno));
    if (nread < 0) {
      // EINVAL (offset overflow) and friends are hard failures, never a
      // silent empty-page materialization.
      LOG(ERROR) << "cannot read page " << pid << ": " << std::strerror(errno);
      return StatusError(StatusCode::kIOError, "cannot read page " +
                                                   std::to_string(pid) + ": " +
                                                   std::strerror(errno));
    }
    if (nread == 0) {
      // Past EOF: a cleanly never-written region; materialize a free page in
      // memory (checksum stays 0 until WriteBack).
      target->PageInit(pid, PageType::kFreePage);
    } else if (std::cmp_less(nread, kPageSize)) {
      LOG(ERROR) << "short read on page " << pid << ": " << nread << "/"
                 << kPageSize << " bytes; possible torn write";
      if (std::cmp_greater_equal(nread, kPageHeaderSize)) {
        // DecodeDisk copies the whole body, so the unwritten tail must be
        // defined memory.
        std::memset(disk_image.data() + nread, 0,
                    kPageSize - static_cast<size_t>(nread));
        const Status decode = target->DecodeDisk(disk_image.data());
        if (decode != Status::kSuccess) {
          target->PageInit(pid, PageType::kUnknown);
          target->format_magic = 0;
        }
        target->checksum = 1;
      } else {
        target->PageInit(pid, PageType::kUnknown);
        target->format_magic = 0;
        target->checksum = 1;
      }
      if (validate) {
        return StatusError(StatusCode::kCorrupt,
                           "short page image: page_id=" + std::to_string(pid));
      }
    } else {
      // Cheap pre-check: a page whose header region holds any non-zero byte
      // cannot be an all-zero fresh region, and every readable page carries a
      // non-zero magic there. Only the (rare) zero-header case pays the full
      // 256 KiB scan; the common path skips one full pass per page read.
      const bool header_has_nonzero =
          std::ranges::any_of(disk_image | std::views::take(kPageHeaderSize),
                              [](char byte) { return byte != 0; });
      bool all_zero = false;
      if (header_has_nonzero) {
        all_zero = false;
      } else {
        all_zero = std::ranges::all_of(disk_image,
                                       [](char byte) { return byte == 0; });
      }
      if (all_zero) {
        target->PageInit(pid, PageType::kFreePage);
        nread = 0;  // Fresh sparse region has no checksum to validate.
      } else {
        const Status decode = target->DecodeDisk(disk_image.data());
        if (decode != Status::kSuccess) {
          // Preserve a recoverable, checksum-invalid placeholder. Recovery
          // opens pages without validation and reconstructs this image from
          // WAL; ordinary readers still hit the validation failure below.
          // The non-zero sentinel checksum distinguishes this corrupt (torn)
          // page from a genuine all-zero fresh region, so the free-page
          // materialization below does NOT silently reclaim live data.
          LOG(ERROR) << "invalid page format on page_id=" << pid << ": "
                     << decode;
          target->PageInit(pid, PageType::kUnknown);
          target->format_magic = 0;
          target->checksum = 1;
        }
      }
    }
  }
  if (std::cmp_equal(nread, kPageSize) && !target->IsValid()) {
    if (target->type == PageType::kUnknown && target->checksum == 0) {
      // A freshly extended file region reads as zeros; uninitialized rather
      // than corrupt. (Genuine zero regions are caught by the all_zero check
      // above with nread reset to 0, so this branch is defensive only.) Real
      // corrupt pages carry the sentinel checksum=1 and fall through to fail
      // closed below.
      target->PageInit(pid, PageType::kFreePage);
    } else if (validate) {
      LOG(ERROR) << "corrupt page checksum on page_id=" << pid
                 << " status=" << Status::kCorrupt;
      return StatusError(
          StatusCode::kCorrupt,
          "corrupt page checksum: page_id=" + std::to_string(pid));
    }
    // Otherwise hand the broken image back verbatim: the recovery manager
    // detects it via IsValid() and rebuilds the page from the log.
  }

  // RecLSN = MAX means a clean page.
  target->recovery_lsn = std::numeric_limits<lsn_t>::max();
  return Status::kSuccess;
}

}  // namespace tinylamb
