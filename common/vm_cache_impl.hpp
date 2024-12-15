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
#ifndef TINYLAMB_VM_CACHE_IMPL_HPP
#define TINYLAMB_VM_CACHE_IMPL_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iosfwd>
#include <mutex>
#include <string>
#include <vector>

namespace tinylamb {

class VMCacheImpl {
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

  friend std::ostream& operator<<(std::ostream& o, const PageState& s);

 public:
  VMCacheImpl(int fd, size_t block_size, size_t memory_capacity,
              size_t offset = 0, size_t file_size = 0);
  ~VMCacheImpl();
  VMCacheImpl(const VMCacheImpl&) = delete;
  VMCacheImpl(VMCacheImpl&&) = delete;
  VMCacheImpl& operator=(const VMCacheImpl&) = delete;
  VMCacheImpl& operator=(VMCacheImpl&&) = delete;
  void Read(void* dst, size_t offset, size_t length) const;
  void Invalidate(size_t offset, size_t length);
  [[nodiscard]] std::string Dump() const;

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
  const size_t block_size_;
  mutable char* buffer_;
  const size_t max_memory_pages_;
  const size_t max_size_;
  const size_t offset_;
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

#endif  // TINYLAMB_VM_CACHE_IMPL_HPP
