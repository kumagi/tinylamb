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

#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>

#include "common/vm_cache_impl.hpp"

namespace tinylamb {

// Byte-oriented 4 KiB-block cache for LSM blob files. Wraps VMCacheImpl.
class Cache final {
  static constexpr size_t kBlockSize = 4UL * 1024;

 public:
  using Lock = VMCacheImpl::PageLock;
  using Locks = VMCacheImpl::Locks;

  // The caller keeps ownership of `fd`: this Cache (and the wrapped
  // VMCacheImpl) never closes it. BlobFile relies on this so that only the
  // Logger ever closes the shared descriptor.
  Cache(int fd, size_t memory_capacity, size_t max_size = 0)
      : impl_(fd, kBlockSize, memory_capacity, 0, max_size, false) {}
  ~Cache() = default;
  Cache(const Cache&) = delete;
  Cache(Cache&&) = delete;
  Cache& operator=(const Cache&) = delete;
  Cache& operator=(Cache&&) = delete;

  [[nodiscard]] std::string ReadAt(size_t offset, size_t length) const {
    return impl_.ReadAt(offset, length);
  }
  Locks ReadAt(size_t offset, size_t length, std::string_view& out) const {
    return impl_.ReadAt(offset, length, out);
  }
  void Copy(void* dst, size_t offset, size_t length) const {
    impl_.Copy(dst, offset, length);
  }
  void Invalidate(size_t offset, size_t length) {
    impl_.Invalidate(offset, length);
  }
  [[nodiscard]] std::string Dump() const { return impl_.Dump(); }

  friend std::ostream& operator<<(std::ostream& o, const Cache& c) {
    o << c.Dump();
    return o;
  }

 private:
  VMCacheImpl impl_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CACHE_HPP
