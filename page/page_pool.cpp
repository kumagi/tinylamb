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
    // Fast hit path: shared lock + atomic pin. LRU Touch is best-effort.
    {
      std::shared_lock shared_latch(pool_latch);
      auto entry = pool_.find(page_id);
      if (entry != pool_.end()) {
        entry->second->pin_count.fetch_add(1, std::memory_order_relaxed);
        Page* const page = entry->second->page.get();
        std::shared_mutex* const page_latch = entry->second->page_latch.get();
        if (cache_hit != nullptr) {
          *cache_hit = true;
        }
        shared_latch.unlock();
        {
          std::unique_lock touch_latch(pool_latch, std::try_to_lock);
          if (touch_latch.owns_lock()) {
            if (auto again = pool_.find(page_id); again != pool_.end()) {
              Touch(again->second);
            }
          }
        }
        return {this, page, page_latch, shared};
      }
    }

    std::unique_lock latch(pool_latch);
    // Recheck under exclusive lock in case another thread installed the page.
    if (auto entry = pool_.find(page_id); entry != pool_.end()) {
      entry->second->pin_count.fetch_add(1, std::memory_order_relaxed);
      Touch(entry->second);
      const LruType::iterator refreshed = pool_.at(page_id);
      Page* const page = refreshed->page.get();
      std::shared_mutex* const page_latch = refreshed->page_latch.get();
      if (cache_hit != nullptr) {
        *cache_hit = true;
      }
      latch.unlock();
      return {this, page, page_latch, shared};
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
        std::scoped_lock file(file_latch_);
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
          pool_.emplace(victim_id, std::prev(pool_lru_.end()));
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
    // the victim in flushing_ (under pool_latch) BEFORE taking file_latch_ to
    // pwrite it. By checking flushing_ only while holding file_latch_, a
    // clear check means any racing write-back of this page id has already
    // finished, or is serialized strictly after our ReadFrom below.
    for (;;) {
      {
        std::scoped_lock file(file_latch_);
        bool pending = false;
        {
          std::shared_lock check(pool_latch);
          pending = flushing_.contains(page_id);
        }
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
      latch.unlock();
      return {this, page, page_latch, shared};
    }

    // Capacity was last validated before the latch was released for I/O; a
    // concurrent miss may have filled the pool meanwhile. Discard this load
    // and retry so the eviction loop runs first.
    if (pool_lru_.size() >= capacity_ &&
        ++attempts < kMaxInstallAttempts) {
      continue;
    }

    Page* const raw_page = new_page.release();
    std::shared_mutex* const raw_latch = new_page_latch.get();
    pool_lru_.emplace_back(raw_page);
    pool_lru_.back().page_latch = std::move(new_page_latch);
    pool_.emplace(page_id, std::prev(pool_lru_.end()));
    latch.unlock();
    // Page content was loaded without holding page_latch, so the caller may
    // take a shared page latch when requested.
    return {this, raw_page, raw_latch, shared};
  }
}

void PagePool::DropAllPages() {
  std::unique_lock latch(pool_latch);
  // Entries still pinned by live PageRefs must outlive this call; splicing
  // them into retired_ keeps their memory alive so a later PageRef
  // destructor never dereferences a freed entry.
  for (auto it = pool_lru_.begin(); it != pool_lru_.end();) {
    if (0 < it->pin_count.load(std::memory_order_relaxed)) {
      retired_.splice(retired_.end(), pool_lru_, it++);
    } else {
      ++it;
    }
  }
  pool_.clear();
  pool_lru_.clear();
}

void PagePool::FlushPageForTest(page_id_t page_id) {
  Page* target = nullptr;
  {
    std::unique_lock latch(pool_latch);
    const auto it = pool_.find(page_id);
    if (it == pool_.end()) {
      return;  // Already evicted.
    }
    target = it->second->page.get();
  }
  // The entry cannot be evicted while this thread's PageRef-free flush runs;
  // test-only callers hold no competing pins, so the raw pointer stays valid.
  std::scoped_lock file(file_latch_);
  WriteBack(target);
}

void PagePool::Unpin(page_id_t page_id) {
  // Fast path: the decrement happens outside the exclusive latch so parallel
  // unpins and hit-path pins never serialize behind eviction bookkeeping.
  std::shared_lock latch(pool_latch);
  const auto entry = pool_.find(page_id);
  if (entry == pool_.end()) {
    return;  // Dropped or retired; nothing to release.
  }
  uint32_t pins = entry->second->pin_count.load(std::memory_order_relaxed);
  while (pins != 0) {
    if (entry->second->pin_count.compare_exchange_weak(
            pins, pins - 1, std::memory_order_release,
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
    *victim = std::move(target->page);
    const uint64_t page_id = (*victim)->PageID();
    pool_lru_.erase(target);
    pool_.erase(page_id);
    return true;
  }
  return false;
}

// Precondition: pool_latch is locked exclusively.
void PagePool::Touch(LruType::iterator it) {
  assert(!pool_latch.try_lock());
  Entry tmp(std::move(*it));
  const page_id_t page_id = tmp.page->PageID();
  pool_lru_.erase(it);
  pool_lru_.push_back(std::move(tmp));
  pool_[page_id] = std::prev(pool_lru_.end());
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
    std::scoped_lock file(file_latch_);
    for (Page* page : dirty) {
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
  off_t offset = 0;
  if (!PageOffset(target->PageID(), &offset)) {
    throw std::runtime_error("page offset out of range: page_id=" +
                             std::to_string(target->PageID()));
  }
  const char* buffer = reinterpret_cast<const char*>(target);
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
  ssize_t nread = 0;
  do {
    nread = ::pread(fd_, target, kPageSize, offset);
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
  } else if (!target->IsValid()) {
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
