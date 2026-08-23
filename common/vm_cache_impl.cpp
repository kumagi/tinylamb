/**
 * Copyright 2024 KUMAZAKI Hiroki
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
#include "vm_cache_impl.hpp"

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <ostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include "common/log_message.hpp"

namespace tinylamb {
namespace {
uint64_t FileSize(int fd) {
  struct stat s {};
  if (::fstat(fd, &s) == -1) {
    throw std::runtime_error(std::string("Cannot get filesize: ") +
                             strerror(errno));
  }
  if (s.st_size < 0) {
    throw std::runtime_error("Negative filesize from fstat");
  }
  return static_cast<uint64_t>(s.st_size);
}

size_t AddressableSize(int fd, size_t offset, size_t file_size) {
  if (file_size != 0) {
    return file_size;
  }
  const uint64_t on_disk = FileSize(fd);
  return on_disk >= offset ? static_cast<size_t>(on_disk - offset) : 0;
}
}  // namespace

std::ostream& operator<<(std::ostream& o, const VMCacheImpl::PageState& s) {
  switch (s) {
    case VMCacheImpl::PageState::kUnknown:
      o << "<Unknown>";
      break;
    case VMCacheImpl::PageState::kEvicted:
      o << "<Evicted>";
      break;
    case VMCacheImpl::PageState::kLocked:
      o << "<Locked>";
      break;
    case VMCacheImpl::PageState::kUnlocked:
      o << "<Unlocked>";
      break;
    case VMCacheImpl::PageState::kMarked:
      o << "<Marked>";
      break;
    case VMCacheImpl::PageState::kLockedAccessed:
      o << "<LockedAccessed>";
      break;
    case VMCacheImpl::PageState::kUnlockedAccessed:
      o << "<UnlockedAccessed>";
      break;
  }
  return o;
}

VMCacheImpl::VMCacheImpl(int fd, size_t block_size, size_t memory_capacity,
                         size_t offset, size_t file_size, bool own_fd)
    : fd_(fd),
      own_fd_(own_fd),
      block_size_(block_size),
      max_memory_pages_((memory_capacity + block_size - 1) / block_size),
      // An explicit `file_size` is the addressable window (e.g. a blob's
      // maximum size, since the file keeps growing); otherwise fall back to
      // the current on-disk size. Always map at least one block so empty
      // files stay usable.
      max_size_(
          std::max<size_t>(AddressableSize(fd, offset, file_size), block_size)),
      offset_(offset),
      meta_(((max_size_ / block_size)) + 1),
      // Degenerated configurations (e.g. memory_capacity < block_size) would
      // otherwise produce zero-sized queues whose front()/pop_front() are UB.
      small_queue_size_(std::max<size_t>(1, (max_memory_pages_ + 9) / 10)),
      main_queue_size_(std::max<size_t>(
          1, max_memory_pages_ - ((max_memory_pages_ + 9) / 10))),
      ghost_queue_size_(std::max<size_t>(
          1, max_memory_pages_ - ((max_memory_pages_ + 9) / 10))) {
  if (memory_capacity == 0) {
    throw std::runtime_error("Cache size is 0");
  }
  buffer_ = reinterpret_cast<char*>(
      ::mmap(nullptr, max_size_, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_PRIVATE | MAP_NORESERVE, -1, 0));
  if (buffer_ == MAP_FAILED) {
    buffer_ = nullptr;
    throw std::runtime_error(std::string("mmap failed: ") + strerror(errno));
  }
  for (auto& m : meta_) {
    m.store(PageState::kEvicted, std::memory_order_relaxed);
  }
  /*
  LOG(TRACE) << "VMCacheImpl: " << max_memory_pages_ << " pages in memory"
             << " total: " << meta_.size()
             << " pages Small: " << small_queue_size_
             << " Main: " << main_queue_size_
             << " Ghost: " << ghost_queue_size_;
             */
}

void VMCacheImpl::Read(void* dst, size_t offset, size_t length) const {
  char* dst_ptr = reinterpret_cast<char*>(dst);
  // Clamp to the mapped window exactly like the string-returning ReadAt();
  // reading past max_size_ would touch unmapped memory.
  if (length == 0 || max_size_ <= offset) {
    return;
  }
  length = std::min(length, max_size_ - offset);
  // NOTE: deliberately the ceil-style formula. For block-aligned offsets it
  // yields 0, producing an extra Fix/Unfix round trip that marks the page as
  // accessed; the S3-FIFO promotion policy (and Dump()-based tests) rely on
  // that observable behavior.
  size_t to_next_boundary =
      (((offset + block_size_ - 1) / block_size_) * block_size_) - offset;
  size_t read_size = std::min(to_next_boundary, length);
  while (0 < length) {
    ReadInPage(dst_ptr, read_size, buffer_ + offset);
    offset += read_size;
    length -= read_size;
    dst_ptr += read_size;
    read_size = std::min(block_size_, length);
  }
}

std::string VMCacheImpl::ReadAt(size_t offset, size_t length) const {
  std::string result(length, '\0');
  Copy(result.data(), offset, length);
  return result;
}

VMCacheImpl::Locks VMCacheImpl::ReadAt(size_t offset, size_t length,
                                       std::string_view& out) const {
  Locks locks;
  if (offset >= max_size_ || length == 0) {
    out = {};
    return locks;
  }
  const size_t take = std::min(length, max_size_ - offset);
  const size_t first_page = offset / block_size_;
  const size_t last_page = (offset + take - 1) / block_size_;
  const size_t last_valid = meta_.empty() ? 0 : meta_.size() - 1;
  const size_t clamped_last = std::min(last_page, last_valid);

  locks.reserve(clamped_last >= first_page ? clamped_last - first_page + 1 : 0);
  for (size_t page = first_page; page <= clamped_last; ++page) {
    FixPage(page);
    locks.push_back(PageLock::Pin(meta_[page]));
  }
  out = std::string_view(&buffer_[offset], take);
  return locks;
}

void VMCacheImpl::Invalidate(size_t offset, size_t length) {
  if (length == 0 || meta_.empty()) {
    return;
  }
  const size_t file_bytes = meta_.size() * block_size_;
  if (offset >= file_bytes) {
    return;
  }
  const size_t end = std::min(offset + length, file_bytes);
  const size_t first_page = offset / block_size_;
  const size_t last_page = (end - 1) / block_size_;
  const size_t last_valid = meta_.size() - 1;
  const size_t clamped_last = std::min(last_page, last_valid);
  for (size_t target = first_page; target <= clamped_last; ++target) {
    InvalidatePage(target);
  }
}

// The `dst` to `length` range must not go over any page boundaries.
void VMCacheImpl::ReadInPage(void* dst, size_t length, void* src) const {
  size_t page = (reinterpret_cast<char*>(src) - buffer_) / block_size_;
  FixPage(page);
  ::memcpy(dst, src, length);
  UnfixPage(page);
}

size_t VMCacheImpl::FindMetaPage(std::atomic<PageState>* page_ptr) const {
  return page_ptr - meta_.data();
}

void VMCacheImpl::EnqueueToSmallFifo(std::atomic<PageState>* page_ptr) const {
  size_t scanned_locked = 0;
  while (small_queue_.size() >= small_queue_size_) {
    std::atomic<PageState>* dequeued = small_queue_.front();
    small_queue_.pop_front();
    assert(dequeued != nullptr);
    for (;;) {
      PageState prev = dequeued->load();
      switch (prev) {
        case PageState::kLocked: {
          // Pinned pages cannot be evicted; rotate and keep looking. If every
          // resident small-fifo page is pinned, allow a temporary overflow.
          small_queue_.push_back(dequeued);
          ++scanned_locked;
          if (scanned_locked >= small_queue_size_) {
            goto enqueue_small;
          }
          dequeued = small_queue_.front();
          small_queue_.pop_front();
          continue;
        }
        case PageState::kUnlocked: {
          if (!dequeued->compare_exchange_weak(prev, PageState::kMarked,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
            continue;
          }
          // Two-phase eviction: MADV_DONTNEED only fires while queue_lock_ is
          // held and the page is still kMarked. A concurrent FixPage that won
          // the kMarked->kLocked race is guaranteed to be waiting for this
          // lock before it reloads the page, so its Activate() pread always
          // lands after our madvise (or wins the race and cancels it).
          if (dequeued->load(std::memory_order_acquire) ==
              PageState::kMarked) {
            Release(FindMetaPage(dequeued));
          }
          EnqueueToGhostFifo(dequeued);
          break;
        }
        case PageState::kLockedAccessed:
          if (!dequeued->compare_exchange_weak(prev, PageState::kLocked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          EnqueueToMainFifo(dequeued);
          break;
        case PageState::kUnlockedAccessed:
          if (!dequeued->compare_exchange_weak(prev, PageState::kUnlocked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          EnqueueToMainFifo(dequeued);
          break;
        case PageState::kMarked:  // Ghost promotion raced the scan; leaving.
        case PageState::kEvicted:  // InvalidatePage retired this page in fifo.
          // The page is leaving anyway; just drop it from the small fifo.
          break;
        case PageState::kUnknown:
        default:
          assert(!"never reach here");
          throw std::runtime_error("VMCacheImpl: unknown page state");
      }
      break;
    }
  }
enqueue_small:
  small_queue_.push_back(page_ptr);
}

// Bounded state-machine re-queue, not a growing recursion: each nested call
// only consumes a page's accessed bit (k*Accessed -> k*) before re-entering,
// so the chain cannot cycle forever.
void VMCacheImpl::EnqueueToMainFifo(  // NOLINT(misc-no-recursion)
    std::atomic<PageState>* page_ptr) const {
  size_t scanned_locked = 0;
  while (main_queue_.size() >= main_queue_size_) {
    std::atomic<PageState>* dequeued = main_queue_.front();
    LOG(TRACE) << "loading page: " << dequeued;
    main_queue_.pop_front();
    assert(dequeued != nullptr);
    for (;;) {
      PageState prev = dequeued->load(std::memory_order_acquire);
      switch (prev) {
        case PageState::kLocked: {
          // Pinned pages cannot be evicted; rotate and keep looking. If every
          // resident main-fifo page is pinned, allow a temporary overflow.
          main_queue_.push_back(dequeued);
          ++scanned_locked;
          if (scanned_locked >= main_queue_size_) {
            goto enqueue_main;
          }
          dequeued = main_queue_.front();
          main_queue_.pop_front();
          continue;
        }
        case PageState::kUnlocked:
          if (!dequeued->compare_exchange_weak(prev, PageState::kEvicted,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
            continue;
          }
          // Two-phase eviction (see EnqueueToSmallFifo): drop the page only
          // under queue_lock_ and only while it is still kEvicted; a racing
          // FixPage reloads strictly after this madvise completes.
          if (dequeued->load(std::memory_order_acquire) ==
              PageState::kEvicted) {
            Release(FindMetaPage(dequeued));
          }
          break;
        case PageState::kLockedAccessed:
          if (!dequeued->compare_exchange_weak(prev, PageState::kLocked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          EnqueueToMainFifo(dequeued);
          break;
        case PageState::kUnlockedAccessed:
          if (!dequeued->compare_exchange_weak(prev, PageState::kUnlocked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          EnqueueToMainFifo(dequeued);
          break;
        case PageState::kMarked:  // Ghost promotion raced the scan; leaving.
        case PageState::kEvicted:  // InvalidatePage retired this page in fifo.
          // The page is leaving anyway; just drop it from the main fifo.
          break;
        case PageState::kUnknown:
        default:
          assert(!"never reach here");
          throw std::runtime_error("VMCacheImpl: unknown page state");
      }
      break;
    }
  }
enqueue_main:
  main_queue_.push_back(page_ptr);
}

void VMCacheImpl::EnqueueToGhostFifo(std::atomic<PageState>* page_ptr) const {
  if (ghost_queue_.size() >= ghost_queue_size_) {
    std::atomic<PageState>* dequeued = ghost_queue_.front();
    ghost_queue_.pop_front();
    assert(dequeued != nullptr);
    for (;;) {
      PageState prev = dequeued->load(std::memory_order_acquire);
      switch (prev) {
        case PageState::kMarked:
          if (!dequeued->compare_exchange_weak(prev, PageState::kEvicted,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          break;
        case PageState::kLocked:
        case PageState::kUnlocked:
        case PageState::kLockedAccessed:
        case PageState::kUnlockedAccessed:
          // TODO: The accessed flag should be removed?
          EnqueueToMainFifo(dequeued);
          break;
        case PageState::kEvicted:
          // InvalidatePage retired this page while it sat in the ghost fifo.
        case PageState::kUnknown:
          // Nothing sensible to requeue; drop the stale entry.
          break;
      }
      break;
    }
  }
  ghost_queue_.push_back(page_ptr);
  assert(ghost_queue_.size() <= ghost_queue_size_);
}

void VMCacheImpl::FixPage(size_t page) const {
  std::atomic<PageState>& target = meta_[page];
  for (;;) {
    PageState state = target.load(std::memory_order_acquire);
    if (state == PageState::kEvicted || state == PageState::kMarked) {
      if (std::atomic_compare_exchange_weak(&target, &state,
                                            PageState::kLocked)) {
        {
          std::scoped_lock lk(queue_lock_);
          if (state == PageState::kEvicted) {
            EnqueueToSmallFifo(&target);
          } else {
            std::erase(ghost_queue_, &target);
            EnqueueToMainFifo(&target);
          }
        }
        Activate(page);
        return;
      }
    } else if (state == PageState::kUnlocked ||
               state == PageState::kUnlockedAccessed) {
      if (std::atomic_compare_exchange_weak(&target, &state,
                                            PageState::kLockedAccessed)) {
        return;
      }
    } else {
      // kLocked/kLockedAccessed: another thread pins this page. Back off so
      // contention degrades to a pause instead of a busy spin.
      std::this_thread::yield();
    }
  }
}

void VMCacheImpl::UnfixPage(size_t page) const {
  const PageState state = meta_[page].load(std::memory_order_relaxed);
  if (state == PageState::kLocked) {
    meta_[page].store(PageState::kUnlocked, std::memory_order_release);
  } else if (state == PageState::kLockedAccessed) {
    meta_[page].store(PageState::kUnlockedAccessed, std::memory_order_release);
  } else {
    assert(!"Invalid state sequence");
    throw std::runtime_error("UnfixPage: invalid state sequence");
  }
}

void VMCacheImpl::InvalidatePage(size_t page) const {
  std::atomic<PageState>& target = meta_[page];
  for (;;) {
    {
      // Retire the page atomically under queue_lock_ (two-phase protocol): a
      // concurrent FixPage either pins the page before our CAS succeeds (we
      // observe the pin and retry later), or observes kEvicted afterwards and
      // must take queue_lock_ to re-register -- so its Activate() pread always
      // lands after the madvise below.  Doing the transition and the queue
      // erase under one lock keeps a re-fixed page from being erased out of
      // every FIFO while resident (permanent tracking loss).
      std::scoped_lock lk(queue_lock_);
      PageState state = target.load(std::memory_order_acquire);
      if (state == PageState::kLocked || state == PageState::kLockedAccessed) {
        // Pinned elsewhere; drop the lock and retry after it is released.
      } else if (state == PageState::kEvicted ||
                 target.compare_exchange_weak(state, PageState::kEvicted,
                                              std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
        std::erase(small_queue_, &target);
        std::erase(main_queue_, &target);
        std::erase(ghost_queue_, &target);
        if (state != PageState::kEvicted) {
          Release(page);
        }
        return;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

void VMCacheImpl::Activate(size_t page) const {
  size_t offset = (page * block_size_);
  size_t rest_size = block_size_;
  while (0 < rest_size) {
    ssize_t read_bytes =
        ::pread(fd_, &buffer_[offset], rest_size, offset + offset_);
    if (read_bytes < 0) {
      // A failed pread must not decrement rest_size below zero (the loop would
      // never terminate); treat it as fatal like the mmap failure path.
      throw std::runtime_error(std::string("pread failed: ") +
                               strerror(errno));
    }
    if (read_bytes == 0) {
      break;
    }
    rest_size -= read_bytes;
    offset += read_bytes;
  }
}

void VMCacheImpl::Release(size_t page) const {
  /*
  LOG(DEBUG) << " release: " << page << " current: " << --activated_pages
             << " | " << Dump();
  //*/
  if (::madvise(&buffer_[page * block_size_], block_size_, MADV_DONTNEED) !=
      0) {
    // Only reachable when block_size_ is not page-aligned or the range is not
    // mapped; the page then silently keeps its RSS.
    LOG(ERROR) << "madvise failed on page " << page << ": " << strerror(errno);
    assert(!"madvise failed");
  }
}

bool VMCacheImpl::SanityCheck() const {
  std::set<std::atomic<PageState>*> pages;
  for (const auto& c : small_queue_) {
    if (pages.contains(c)) {
      assert(!"Duplicate fifo entry");
      throw std::runtime_error("SanityCheck: duplicate entry in small fifo");
    }
    pages.emplace(c);
  }
  for (const auto& c : main_queue_) {
    if (pages.contains(c)) {
      assert(!"Duplicate fifo entry");
      throw std::runtime_error("SanityCheck: duplicate entry in main fifo");
    }
    pages.emplace(c);
  }
  for (const auto& c : ghost_queue_) {
    if (pages.contains(c)) {
      assert(!"Duplicate fifo entry");
      throw std::runtime_error("SanityCheck: duplicate entry in ghost fifo");
    }
    pages.emplace(c);
  }
  return true;
}

std::string VMCacheImpl::Dump() const {
  std::scoped_lock lk(queue_lock_);
  std::stringstream ss;
  ss << "[";
  for (size_t i = 0; i < small_queue_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << FindMetaPage(small_queue_[i]);
  }
  ss << "] {";
  for (size_t i = 0; i < main_queue_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << FindMetaPage(main_queue_[i]);
  }
  ss << "} [";
  for (size_t i = 0; i < ghost_queue_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << FindMetaPage(ghost_queue_[i]);
  }
  ss << "]";
  SanityCheck();
  return ss.str();
}

VMCacheImpl::~VMCacheImpl() {
  if (buffer_ != nullptr &&
      ::munmap(buffer_, max_size_) != 0) {
    // Destructors must not throw; report loudly and continue shutdown.
    LOG(ERROR) << "Destructing cache: " << strerror(errno);
    assert(!"munmap failed");
  }
  if (own_fd_) {
    ::close(fd_);
  }
}
}  // namespace tinylamb
