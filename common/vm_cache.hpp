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
#ifndef TINYLAMB_VM_CACHE_HPP
#define TINYLAMB_VM_CACHE_HPP

#include <algorithm>
#include <cstddef>
#include <numeric>
#include <ostream>
#include <string>

#include "common/vm_cache_impl.hpp"

namespace tinylamb {
template <typename T>
class VMCache {
 public:
  // Block boundaries double as MADV_DONTNEED ranges, so the block size must be
  // a page multiple (otherwise madvise fails with EINVAL and evicted pages
  // keep their RSS).  Returns the page-aligned size nearest to `around` that
  // still fits at least one `T`.
  static size_t FindNearestSize(size_t target = sizeof(T),
                                size_t around = 4096) {
    constexpr size_t kBlockSizeForAlign = 4096;
    const size_t min_size =
        ((target + kBlockSizeForAlign - 1) / kBlockSizeForAlign) *
        kBlockSizeForAlign;
    const size_t rounded =
        ((around + kBlockSizeForAlign / 2) / kBlockSizeForAlign) *
        kBlockSizeForAlign;
    return std::max(std::max(min_size, rounded), kBlockSizeForAlign);
  }
  VMCache(int fd, size_t memory_capacity, size_t offset = 0,
          size_t file_size = 0)
      : cache_(fd, FindNearestSize(), memory_capacity, offset, file_size) {}
  VMCache(const VMCache&) = delete;
  VMCache(VMCache&&) = delete;
  VMCache& operator=(const VMCache&) = delete;
  VMCache& operator=(VMCache&&) = delete;
  ~VMCache() = default;

  void Read(T* dst, size_t offset, size_t size) const {
    cache_.Read(dst, offset * sizeof(T), size * sizeof(T));
  }
  void Invalidate(size_t offset, size_t length) {
    // Element units like Read(): the offset was previously passed through
    // as raw bytes while the length was scaled, so any non-zero offset
    // invalidated the wrong range.
    cache_.Invalidate(offset * sizeof(T), length * sizeof(T));
  }
  [[nodiscard]] std::string Dump() const { return cache_.Dump(); }

  friend std::ostream& operator<<(std::ostream& o, const VMCache<T>& c) {
    o << c.Dump();
    return o;
  }

 private:
  VMCacheImpl cache_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_VM_CACHE_HPP
