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
#ifndef TINYLAMB_CACHE_HPP
#define TINYLAMB_CACHE_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "common/ring_buffer.hpp"

namespace tinylamb {

class Cache final {
  static constexpr size_t kBlockSize = 4UL * 1024;

  enum class PageState : std::uint8_t {
    kUnknown = 0,

    // No data cached.
    kEvicted = 1,

    // The data cached and currently using.
    kLocked = 2,

    // The data cached and currently not used now.
    kUnlocked = 3,

    // The data cached and waiting for evict.
    kMarked = 4,

    // The data cached and accessed at least twice and currently using.
    kLockedAccessed = 5,

    // The data cached and accessed at least twice and not used now.
    kUnlockedAccessed = 6,
  };

  friend std::ostream& operator<<(std::ostream& o, const PageState& s) {
    switch (s) {
      case PageState::kUnknown:
        o << "<Unknown>";
        break;
      case PageState::kEvicted:
        o << "<Evicted>";
        break;
      case PageState::kLocked:
        o << "<Locked>";
        break;
      case PageState::kUnlocked:
        o << "<Unlocked>";
        break;
      case PageState::kMarked:
        o << "<Marked>";
        break;
      case PageState::kLockedAccessed:
        o << "<LockedAccessed>";
        break;
      case PageState::kUnlockedAccessed:
        o << "<UnlockedAccessed>";
        break;
    }
    return o;
  }

 public:
  class Lock {
   public:
    ~Lock() noexcept {
      if (locked_page_ != nullptr) {
        locked_page_->store(PageState::kUnlocked, std::memory_order_release);
      }
    }
    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;
    Lock(Lock&& other) noexcept : locked_page_(other.locked_page_) {
      other.locked_page_ = nullptr;
    }
    Lock& operator=(Lock&& other) noexcept {
      if (this != &other) {
        if (locked_page_ != nullptr) {
          locked_page_->store(PageState::kUnlocked, std::memory_order_release);
        }
        locked_page_ = other.locked_page_;
        other.locked_page_ = nullptr;
      }
      return *this;
    }

   private:
    friend class Cache;
    std::atomic<PageState>* locked_page_{nullptr};
    explicit Lock(std::atomic<PageState>& target) : locked_page_(&target) {}
    static Lock Pin(std::atomic<PageState>& target) { return Lock(target); }
  };
  typedef std::vector<Lock> Locks;

  Cache(int fd, size_t memory_capacity, size_t max_size = 0);
  ~Cache();
  Cache(const Cache&) = delete;
  Cache(Cache&&) = delete;
  Cache& operator=(const Cache&) = delete;
  Cache& operator=(Cache&&) = delete;

  std::string ReadAt(size_t offset, size_t length) const;
  Locks ReadAt(size_t offset, size_t length, std::string_view& out) const;
  void Copy(void* dst, size_t offset, size_t length) const;
  void Invalidate(size_t offset, size_t length);

  std::string Dump() const;

  friend std::ostream& operator<<(std::ostream& o, const Cache& c) {
    o << c.Dump();
    return o;
  }

 private:
  void ReadInPage(void* dst, size_t length, void* src) const;
  void FixPage(size_t page) const;
  void UnfixPage(size_t page) const;
  void InvalidatePage(size_t page) const;
  void Activate(size_t page) const;
  void Release(size_t page) const;

  size_t FindMetaPage(std::atomic<PageState>* page_ptr) const;
  void EnqueueToSmallFifo(std::atomic<PageState>* page_ptr) const;
  void EnqueueToMainFifo(std::atomic<PageState>* page_ptr) const;
  void EnqueueToGhostFifo(std::atomic<PageState>* page_ptr) const;

  bool SanityCheck() const;

  int fd_;
  mutable char* buffer_;
  const size_t max_memory_pages_;
  const size_t max_size_;
  mutable std::vector<std::atomic<PageState>> meta_;

  mutable std::mutex queue_lock_;
  const size_t small_queue_size_;
  const size_t main_queue_size_;
  const size_t ghost_queue_size_;
  mutable std::deque<std::atomic<PageState>*> small_queue_;
  mutable std::deque<std::atomic<PageState>*> main_queue_;
  mutable std::deque<std::atomic<PageState>*> ghost_queue_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CACHE_HPP
