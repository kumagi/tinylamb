/**cache_
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
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "index/lsm_detail/cache.hpp"
#include "recovery/logger.hpp"

namespace tinylamb {

class BlobFile final {
 public:
  BlobFile(const std::filesystem::path& path,
           size_t memory_capacity = 128 * 1024 * 1024,
           size_t max_filesize = 1024LLU * 1024 * 1024);

  BlobFile(BlobFile&& o) = delete;
  BlobFile(const BlobFile& o) = delete;
  BlobFile& operator=(BlobFile&& o) = delete;
  BlobFile& operator=(const BlobFile& o) = delete;
  ~BlobFile() = default;

  [[nodiscard]] std::string ReadAt(size_t offset, size_t length) const;
  [[nodiscard]] Cache::Locks ReadAt(size_t, std::string_view& out) const;
  lsn_t Append(std::string_view payload) {
    return file_writer_.AddLog(payload);
  }
  [[nodiscard]] lsn_t Written() const { return file_writer_.CommittedLSN(); }

 private:
  Logger file_writer_;
  Cache cache_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BLOB_FILE_HPP