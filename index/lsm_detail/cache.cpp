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
#include <cstddef>
#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
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
      max_memory_pages_(memory_capacity / kBlockSize + 1),
      max_size_(max_size != 0 ? max_size : FileSize(fd)),
      meta_(max_size_ / kBlockSize + 1),
      small_queue_((max_memory_pages_ + 9) / 10),
      main_queue_(max_memory_pages_ - small_queue_.Capacity()),
      ghost_queue_(main_queue_.Capacity()) {
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
             << " pages Small: " << small_queue_.Capacity()
             << " Main: " << main_queue_.Capacity()
             << " Ghost: " << ghost_queue_.Capacity();
             */
}

std::string Cache::ReadAt(size_t offset, size_t length) const {
  std::string result(length, '\0');
  Copy(result.data(), offset, length);
  return result;
}

Cache::Locks Cache::ReadAt(size_t offset, size_t length,
                           std::string_view& out) const {
  const size_t first_page = offset / kBlockSize;
  const size_t last_page = (offset + length) / kBlockSize;
  const size_t blocks = last_page - first_page + 1;

  Cache::Locks locks;
  locks.reserve(blocks);
  for (size_t page = first_page; page <= last_page; ++page) {
    FixPage(page);
    locks.push_back(meta_[page]);
  }
  out = std::string_view(&buffer_[offset], length);
  return locks;
}

void Cache::Copy(void* dst, size_t offset, size_t length) const {
  char* dst_ptr = reinterpret_cast<char*>(dst);
  size_t copied = 0;
  size_t to_next_boundary =
      ((offset + kBlockSize - 1) / kBlockSize) * kBlockSize - offset;
  size_t read_size = std::min(to_next_boundary, length);
  while (0 < length) {
    size_t original = length;
    ReadInPage(dst_ptr + copied, read_size, buffer_ + offset + copied);
    copied += read_size;
    length -= read_size;
    read_size = std::min(kBlockSize, length);
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
  if (small_queue_.IsFull()) {
    std::atomic<PageState>* dequeued = nullptr;
    small_queue_.Dequeue(&dequeued);
    assert(dequeued != nullptr);
    for (;;) {
      PageState prev = dequeued->load();
      switch (prev) {
        case PageState::kLocked: {
          EnqueueToSmallFifo(dequeued);
          small_queue_.Dequeue(&dequeued);
          continue;
        }
        case PageState::kUnlocked: {
          if (!dequeued->compare_exchange_weak(prev, PageState::kMarked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          ReleasePage(FindMetaPage(dequeued));
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
  small_queue_.Enqueue(page_ptr);
}

void Cache::EnqueueToMainFifo(std::atomic<PageState>* page_ptr) const {
  if (main_queue_.IsFull()) {
    std::atomic<PageState>* dequeued = nullptr;
    main_queue_.Dequeue(&dequeued);
    assert(dequeued != nullptr);
    for (;;) {
      PageState prev = dequeued->load(std::memory_order_acquire);
      switch (prev) {
        case PageState::kLocked:
          EnqueueToMainFifo(dequeued);
          main_queue_.Dequeue(&dequeued);
          continue;
        case PageState::kUnlocked:
          if (!dequeued->compare_exchange_weak(prev, PageState::kEvicted,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          ReleasePage(FindMetaPage(dequeued));
          break;
        case PageState::kLockedAccessed:
          if (!dequeued->compare_exchange_weak(prev, PageState::kLocked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          main_queue_.Enqueue(dequeued);
          break;
        case PageState::kUnlockedAccessed:
          if (!dequeued->compare_exchange_weak(prev, PageState::kUnlocked,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
            continue;
          }
          main_queue_.Enqueue(dequeued);
          break;
        case PageState::kMarked:
          LOG(ERROR) << "Already marked!?";
          break;
        case PageState::kEvicted:
          LOG(ERROR) << "Evicted Page inside main fifo?!?: "
                     << FindMetaPage(page_ptr);
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
  main_queue_.Enqueue(page_ptr);
}

void Cache::EnqueueToGhostFifo(std::atomic<PageState>* page_ptr) const {
  if (main_queue_.IsFull()) {
    std::atomic<PageState>* dequeued = nullptr;
    ghost_queue_.Dequeue(&dequeued);
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
        case PageState::kEvicted:
        case PageState::kLocked:
        case PageState::kUnlocked:
        case PageState::kLockedAccessed:
        case PageState::kUnlockedAccessed:
        case PageState::kUnknown:
          break;
      }
      break;
    }
  }
  ghost_queue_.Enqueue(page_ptr);
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
          } else if (state == PageState::kMarked) {
            EnqueueToMainFifo(&target);
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

static int activated_pages = 0;

void Cache::Activate(size_t page) const {
  /*
  LOG(TRACE) << "activate: " << page << " length: " << kBlockSize
             << " current: " << ++activated_pages;
             */
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

void Cache::ReleasePage(size_t page) const {
  // LOG(WARN) << "release: " << page << " current: " << --activated_pages;
  ::madvise(&buffer_[page * kBlockSize], kBlockSize, MADV_DONTNEED);
}

Cache::~Cache() {
  if (::munmap(buffer_, max_size_) != 0) {
    LOG(FATAL) << "Destructing cache: " << strerror(errno);
  }
}
}  // namespace tinylamb