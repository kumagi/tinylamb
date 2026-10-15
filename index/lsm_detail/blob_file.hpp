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

#ifndef TINYLAMB_BLOB_FILE_HPP
#define TINYLAMB_BLOB_FILE_HPP

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "index/lsm_detail/cache.hpp"

namespace tinylamb {

class BlobFile final {
 public:
  // Opens (creating) the blob file and the read cache over it.
  static StatusOr<std::unique_ptr<BlobFile>> Create(
      const std::filesystem::path& path,
      size_t memory_capacity = size_t{128} * 1024 * 1024,
      size_t max_filesize = 1024LLU * 1024 * 1024);

  BlobFile(BlobFile&& o) = delete;
  BlobFile(const BlobFile& o) = delete;
  BlobFile& operator=(BlobFile&& o) = delete;
  BlobFile& operator=(const BlobFile& o) = delete;
  ~BlobFile();

  [[nodiscard]] StatusOr<std::string> ReadAt(size_t offset,
                                             size_t length) const;
  [[nodiscard]] StatusOr<Cache::Locks> ReadAt(size_t offset,
                                              std::string_view& out) const;
  StatusOr<lsn_t> Append(std::string_view payload);
  [[nodiscard]] lsn_t Written() const {
    return written_.load(std::memory_order_acquire);
  }
  [[nodiscard]] Status Flush() const;
  Status Sync();

  friend std::ostream& operator<<(std::ostream& o, const BlobFile& b) {
    o << "BlobFile(written=" << b.Written() << ", cache=" << *b.cache_ << ")";
    return o;
  }

 private:
  BlobFile(int fd, lsn_t written, size_t max_filesize,
           std::unique_ptr<Cache> cache);

  int fd_ = -1;
  std::atomic<lsn_t> written_{0};
  size_t max_filesize_ = 0;
  std::unique_ptr<Cache> cache_;
  mutable std::mutex writer_lock_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BLOB_FILE_HPP
