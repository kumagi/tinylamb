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
#include <cstring>
#include <mutex>
#include <ostream>
#include <set>
#include <sstream>
#include <string>
#include <thread>

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
                         size_t offset, size_t file_size)
    : fd_(fd),
      block_size_(block_size),
      max_memory_pages_((memory_capacity + block_size - 1) / block_size),
      max_size_(file_size != 0 ? file_size - offset : FileSize(fd) - file_size),
      offset_(offset),
      meta_(((max_size_ / block_size)) + 1),
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
  LOG(TRACE) << "VMCacheImpl: " << max_memory_pages_ << " pages in memory"
             << " total: " << meta_.size()
             << " pages Small: " << small_queue_size_
             << " Main: " << main_queue_size_
             << " Ghost: " << ghost_queue_size_;
             */
}

void VMCacheImpl::Read(void* dst, size_t offset, size_t length) const {
  char* dst_ptr = reinterpret_cast<char*>(dst);
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

void VMCacheImpl::Invalidate(size_t offset, size_t length) {
  for (size_t target = offset / block_size_;
       target <= (offset + length) / block_size_ && target < max_size_;
       ++target) {
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
  while (small_queue_.size() == small_queue_size_) {
    std::atomic<PageState>* dequeued = small_queue_.front();
    small_queue_.pop_front();
    assert(dequeued != nullptr);
    for (;;) {
      PageState prev = dequeued->load();
      switch (prev) {
        case PageState::kLocked: {
          EnqueueToSmallFifo(dequeued);
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
          Release(FindMetaPage(dequeued));
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
  small_queue_.push_back(page_ptr);
  assert(small_queue_.size() <= small_queue_size_);
}

void VMCacheImpl::EnqueueToMainFifo(std::atomic<PageState>* page_ptr) const {
  while (main_queue_.size() >= main_queue_size_) {
    std::atomic<PageState>* dequeued = main_queue_.front();
    LOG(TRACE) << "loading page: " << dequeued;
    main_queue_.pop_front();
    assert(dequeued != nullptr);
    for (;;) {
      PageState prev = dequeued->load(std::memory_order_acquire);
      switch (prev) {
        case PageState::kLocked:
          EnqueueToMainFifo(dequeued);
          dequeued = main_queue_.front();
          main_queue_.pop_front();
          continue;
        case PageState::kUnlocked:
          if (!dequeued->compare_exchange_weak(prev, PageState::kEvicted,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          Release(FindMetaPage(dequeued));
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
  main_queue_.push_back(page_ptr);
  assert(main_queue_.size() <= main_queue_size_);
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

void VMCacheImpl::UnfixPage(size_t page) const {
  const PageState state = meta_[page].load(std::memory_order_relaxed);
  if (state == PageState::kLocked) {
    meta_[page].store(PageState::kUnlocked, std::memory_order_release);
  } else if (state == PageState::kLockedAccessed) {
    meta_[page].store(PageState::kUnlockedAccessed, std::memory_order_release);
  } else {
    LOG(FATAL) << "Invalid state sequence: " << (int)state;
  }
}

void VMCacheImpl::InvalidatePage(size_t page) const {
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

void VMCacheImpl::Activate(size_t page) const {
  size_t offset = (page * block_size_);
  size_t rest_size = block_size_;
  while (0 < rest_size) {
    ssize_t read_bytes =
        ::pread(fd_, &buffer_[offset], rest_size, offset + offset_);
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

void VMCacheImpl::Release(size_t page) const {
  /*
  LOG(DEBUG) << " release: " << page << " current: " << --activated_pages
             << " | " << Dump();
  //*/
  ::madvise(&buffer_[page * block_size_], block_size_, MADV_DONTNEED);
}

bool VMCacheImpl::SanityCheck() const {
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

std::string VMCacheImpl::Dump() const {
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
  if (::munmap(buffer_, max_size_) != 0) {
    LOG(FATAL) << "Destructing cache: " << strerror(errno);
  }
  ::close(fd_);
}
}  // namespace tinylamb
