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

#include <cstddef>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "index/lsm_detail/cache.hpp"
#include "recovery/logger.hpp"

namespace tinylamb {

class BlobFile final {
 public:
  BlobFile(const std::filesystem::path& path,
           size_t memory_capacity = size_t{128} * 1024 * 1024,
           size_t max_filesize = 1024LLU * 1024 * 1024);

  BlobFile(BlobFile&& o) = delete;
  BlobFile(const BlobFile& o) = delete;
  BlobFile& operator=(BlobFile&& o) = delete;
  BlobFile& operator=(const BlobFile& o) = delete;
  ~BlobFile() = default;

  [[nodiscard]] std::string ReadAt(size_t offset, size_t length) const;
  [[nodiscard]] Cache::Locks ReadAt(size_t, std::string_view& out) const;
  lsn_t Append(std::string_view payload);
  [[nodiscard]] lsn_t Written() const { return file_writer_.CommittedLSN(); }
  void Flush() const {
    const lsn_t lsn = file_writer_.BufferedLSN();
    while (file_writer_.CommittedLSN() < lsn) {
      // A dead writer would stall this loop forever; surface its error.
      if (file_writer_.Failed()) {
        throw std::runtime_error("BlobFile flush failed: " +
                                 std::string(std::strerror(
                                     file_writer_.ErrorNumber())));
      }
      std::this_thread::yield();
    }
  }

  friend std::ostream& operator<<(std::ostream& o, const BlobFile& b) {
    o << "BlobFile(written=" << b.Written() << ", cache=" << b.cache_ << ")";
    return o;
  }

 private:
  // Member order matters: `file_writer_` owns the fd, `cache_` only borrows
  // it (Cache never closes), so the Logger is the sole closer of the file.
  Logger file_writer_;
  Cache cache_;
  std::mutex writer_lock_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BLOB_FILE_HPP
