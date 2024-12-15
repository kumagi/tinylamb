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

#include <cstddef>
#include <numeric>
#include <string>

#include "common/vm_cache_impl.hpp"

namespace tinylamb {
template <typename T>
class VMCache {
 public:
  size_t FindNearestSize(size_t target = sizeof(T), size_t around = 4096) {
    size_t upper = target * ((around + target - 1) / target);
    size_t lower = target * (around / target);
    if (upper - around < lower - around) {
      return upper;
    }
    return lower;
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
    cache_.Invalidate(offset, length * sizeof(T));
  }
  [[nodiscard]] std::string Dump() const { return cache_.Dump(); }

 private:
  VMCacheImpl cache_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_VM_CACHE_HPP
