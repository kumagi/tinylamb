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
#include "index/lsm_detail/blob_file.hpp"
#include <endian.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {
BlobFile::BlobFile(const std::filesystem::path& path, size_t memory_capacity,
                   size_t max_filesize)
    : file_writer_(path),
      cache_(file_writer_.Fd(), memory_capacity, max_filesize) {}

std::string BlobFile::ReadAt(size_t offset, size_t length) const {
  // Appends are asynchronous. A reader on the same BlobFile must nevertheless
  // observe every offset already returned by Append().
  Flush();
  return cache_.ReadAt(offset, length);
}

Cache::Locks BlobFile::ReadAt(size_t offset, std::string_view& out) const {
  Flush();
  out = {};
  constexpr size_t kHeaderSize = sizeof(int32_t);
  int32_t key_size = 0;
  cache_.Copy(&key_size, offset, kHeaderSize);
  key_size = be32toh(key_size);
  if (key_size < 0) {
    // Disk-derived length is bogus; refuse instead of propagating garbage.
    return {};
  }
  return cache_.ReadAt(offset + kHeaderSize, key_size, out);
}

lsn_t BlobFile::Append(std::string_view payload) {
  std::scoped_lock<std::mutex> lk(writer_lock_);
  size_t before = file_writer_.BufferedLSN();
  lsn_t lsn = file_writer_.AddLog(payload);
  cache_.Invalidate(before, payload.length());
  return lsn;
}
}  // namespace tinylamb
