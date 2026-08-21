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
#include "index/lsm_detail/cache.hpp"

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/log_message.hpp"

namespace tinylamb {
namespace {
int FileSize(int fd) {
  struct stat s;
  if (::fstat(fd, &s) == -1) {
    LOG(FATAL) << "Cannot get filesize: " << strerror(errno);
    return -1;
  }
  return s.st_size;
}
}  // namespace

Cache::Cache(int fd, size_t memory_capacity, size_t max_size)
    : fd_(fd),
      max_memory_pages_((memory_capacity + kBlockSize - 1) / kBlockSize),
      max_size_(max_size != 0 ? max_size : FileSize(fd)),
      meta_((max_size_ / kBlockSize) + 1),
      small_queue_size_((max_memory_pages_ + 9) / 10),
      main_queue_size_(max_memory_pages_ - small_queue_size_),
      ghost_queue_size_(max_memory_pages_ - small_queue_size_) {
  if (memory_capacity == 0) {
    LOG(FATAL) << "Cache size is 0";
  }
  buffer_ = reinterpret_cast<char*>(
      ::mmap(nullptr, max_size_, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_PRIVATE | MAP_NORESERVE, -1, 0));
  if (buffer_ == nullptr) {
    LOG(FATAL) << strerror(errno);
  }
  for (auto& m : meta_) {
    m.store(PageState::kEvicted, std::memory_order_relaxed);
  }
  /*
  LOG(TRACE) << "Cache: " << max_memory_pages_ << " pages in memory"
             << " total: " << meta_.size()
             << " pages Small: " << small_queue_size_
             << " Main: " << main_queue_size_
             << " Ghost: " << ghost_queue_size_;
             */
}

std::string Cache::ReadAt(size_t offset, size_t length) const {
  std::string result(length, '\0');
  Copy(result.data(), offset, length);
  return result;
}

Cache::Locks Cache::ReadAt(size_t offset, size_t length,
                           std::string_view& out) const {
  Cache::Locks locks;
  if (offset >= max_size_ || length == 0) {
    out = {};
    return locks;
  }
  const size_t take = std::min(length, max_size_ - offset);
  const size_t first_page = offset / kBlockSize;
  const size_t last_page = (offset + take - 1) / kBlockSize;
  const size_t last_valid = meta_.empty() ? 0 : meta_.size() - 1;
  const size_t clamped_last = std::min(last_page, last_valid);

  locks.reserve(clamped_last >= first_page ? clamped_last - first_page + 1 : 0);
  for (size_t page = first_page; page <= clamped_last; ++page) {
    FixPage(page);
    locks.push_back(Lock::Pin(meta_[page]));
  }
  out = std::string_view(&buffer_[offset], take);
  return locks;
}

void Cache::Copy(void* dst, size_t offset, size_t length) const {
  char* dst_ptr = reinterpret_cast<char*>(dst);
  size_t copied = 0;
  size_t to_next_boundary =
      (((offset + kBlockSize - 1) / kBlockSize) * kBlockSize) - offset;
  size_t read_size = std::min(to_next_boundary, length);
  while (0 < length) {
    ReadInPage(dst_ptr + copied, read_size, buffer_ + offset + copied);
    copied += read_size;
    length -= read_size;
    read_size = std::min(kBlockSize, length);
  }
}

void Cache::Invalidate(size_t offset, size_t length) {
  if (length == 0 || offset >= max_size_ || meta_.empty()) {
    return;
  }
  const size_t end = std::min(offset + length, max_size_);
  const size_t first_page = offset / kBlockSize;
  const size_t last_page = (end - 1) / kBlockSize;
  const size_t last_valid = meta_.size() - 1;
  const size_t clamped_last = std::min(last_page, last_valid);
  for (size_t target = first_page; target <= clamped_last; ++target) {
    InvalidatePage(target);
  }
}

// The `dst` to `length` range must not go over any page boundaries.
void Cache::ReadInPage(void* dst, size_t length, void* src) const {
  size_t page = (reinterpret_cast<char*>(src) - buffer_) / kBlockSize;
  FixPage(page);
  ::memcpy(dst, src, length);
  UnfixPage(page);
}

size_t Cache::FindMetaPage(std::atomic<PageState>* page_ptr) const {
  return page_ptr - meta_.data();
}

void Cache::EnqueueToSmallFifo(std::atomic<PageState>* page_ptr) const {
  size_t scanned_locked = 0;
  while (small_queue_.size() == small_queue_size_) {
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
            scanned_locked = 0;
            goto enqueue_small;
          }
          dequeued = small_queue_.front();
          small_queue_.pop_front();
          continue;
        }
        case PageState::kUnlocked: {
          scanned_locked = 0;
          if (!dequeued->compare_exchange_weak(prev, PageState::kMarked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          Release(FindMetaPage(dequeued));
          EnqueueToGhostFifo(dequeued);
          break;
        }
        case PageState::kLockedAccessed:
          scanned_locked = 0;
          if (!dequeued->compare_exchange_weak(prev, PageState::kLocked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          EnqueueToMainFifo(dequeued);
          break;
        case PageState::kUnlockedAccessed:
          scanned_locked = 0;
          if (!dequeued->compare_exchange_weak(prev, PageState::kUnlocked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          EnqueueToMainFifo(dequeued);
          break;
        case PageState::kMarked:
          LOG(ERROR) << "Already marked!?";
          break;
        case PageState::kEvicted:
          LOG(ERROR) << "Evicted Page inside small fifo?!?";
          break;
        case PageState::kUnknown:
        default:
          LOG(FATAL) << "never reach here";
          _exit(1);
          break;
      }
      break;
    }
  }
enqueue_small:
  small_queue_.push_back(page_ptr);
}

void Cache::EnqueueToMainFifo(std::atomic<PageState>* page_ptr) const {
  size_t scanned_locked = 0;
  while (main_queue_.size() >= main_queue_size_) {
    std::atomic<PageState>* dequeued = main_queue_.front();
    main_queue_.pop_front();
    assert(dequeued != nullptr);
    for (;;) {
      PageState prev = dequeued->load(std::memory_order_acquire);
      switch (prev) {
        case PageState::kLocked: {
          main_queue_.push_back(dequeued);
          ++scanned_locked;
          if (scanned_locked >= main_queue_size_) {
            scanned_locked = 0;
            goto enqueue_main;
          }
          dequeued = main_queue_.front();
          main_queue_.pop_front();
          continue;
        }
        case PageState::kUnlocked:
          scanned_locked = 0;
          if (!dequeued->compare_exchange_weak(prev, PageState::kEvicted,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          Release(FindMetaPage(dequeued));
          break;
        case PageState::kLockedAccessed:
          scanned_locked = 0;
          if (!dequeued->compare_exchange_weak(prev, PageState::kLocked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          EnqueueToMainFifo(dequeued);
          break;
        case PageState::kUnlockedAccessed:
          scanned_locked = 0;
          if (!dequeued->compare_exchange_weak(prev, PageState::kUnlocked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          EnqueueToMainFifo(dequeued);
          break;
        case PageState::kMarked:
          LOG(ERROR) << "Already marked!?";
          break;
        case PageState::kEvicted:
          LOG(ERROR) << "Evicted Page inside main fifo?!?: "
                     << FindMetaPage(page_ptr);
          assert(false);
          break;
        case PageState::kUnknown:
        default:
          LOG(FATAL) << "never reach here";
          _exit(1);
          break;
      }
      break;
    }
  }
enqueue_main:
  main_queue_.push_back(page_ptr);
}

void Cache::EnqueueToGhostFifo(std::atomic<PageState>* page_ptr) const {
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
        case PageState::kUnknown:
          LOG(FATAL) << "unexpected ghost path: " << prev << " for page "
                     << FindMetaPage(page_ptr);
          assert(false);
          break;
      }
      break;
    }
  }
  ghost_queue_.push_back(page_ptr);
  assert(ghost_queue_.size() <= ghost_queue_size_);
}

void Cache::FixPage(size_t page) const {
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
          }
        }
        Activate(page);
        return;
      }
    } else if (state == PageState::kMarked || state == PageState::kUnlocked ||
               state == PageState::kUnlockedAccessed) {
      if (std::atomic_compare_exchange_weak(&target, &state,
                                            PageState::kLockedAccessed)) {
        return;
      }
    }
  }
}

void Cache::UnfixPage(size_t page) const {
  const PageState state = meta_[page].load(std::memory_order_relaxed);
  if (state == PageState::kLocked) {
    meta_[page].store(PageState::kUnlocked, std::memory_order_release);
  } else if (state == PageState::kLockedAccessed) {
    meta_[page].store(PageState::kUnlockedAccessed, std::memory_order_release);
  } else {
    LOG(FATAL) << "Invalid state sequence: " << (int)state;
  }
}

void Cache::InvalidatePage(size_t page) const {
  std::atomic<PageState>& target = meta_[page];
  for (;;) {
    PageState state = target.load(std::memory_order_acquire);
    if (state == PageState::kLocked || state == PageState::kLockedAccessed) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }
    if (state == PageState::kEvicted ||
        target.compare_exchange_weak(state, PageState::kEvicted,
                                     std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
      break;
    }
  }
}

void Cache::Activate(size_t page) const {
  size_t offset = page * kBlockSize;
  size_t rest_size = kBlockSize;
  while (0 < rest_size) {
    ssize_t read_bytes = ::pread(fd_, &buffer_[offset], rest_size, offset);
    if (read_bytes < 0) {
      LOG(ERROR) << strerror(errno);
    }
    if (read_bytes == 0) {
      break;
    }
    rest_size -= read_bytes;
    offset += read_bytes;
  }
}

void Cache::Release(size_t page) const {
  ::madvise(&buffer_[page * kBlockSize], kBlockSize, MADV_DONTNEED);
}

bool Cache::SanityCheck() const {
  std::set<std::atomic<PageState>*> pages;
  for (const auto& c : small_queue_) {
    if (pages.contains(c)) {
      LOG(FATAL) << "Duplicate: " << FindMetaPage(c);
    }
    assert(!pages.contains(c));
    pages.emplace(c);
  }
  for (const auto& c : main_queue_) {
    if (pages.contains(c)) {
      LOG(FATAL) << "Duplicate: " << FindMetaPage(c);
    }
    assert(!pages.contains(c));
    pages.emplace(c);
  }
  for (const auto& c : ghost_queue_) {
    if (pages.contains(c)) {
      LOG(FATAL) << "Duplicate: " << FindMetaPage(c);
    }
    assert(!pages.contains(c));
    pages.emplace(c);
  }
  return true;
}

std::string Cache::Dump() const {
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

Cache::~Cache() {
  if (::munmap(buffer_, max_size_) != 0) {
    LOG(FATAL) << "Destructing cache: " << strerror(errno);
  }
  ::close(fd_);
}
}  // namespace tinylamb
