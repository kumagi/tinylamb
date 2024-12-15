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

#include <bits/types/struct_iovec.h>
#include <bits/xopen_lim.h>
#include <endian.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "cache.hpp"
#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

BlobFile::BlobFile(const std::filesystem::path& path, size_t memory_capacity,
                   size_t max_filesize)
    : file_writer_(path),
      cache_(file_writer_.Fd(), memory_capacity, max_filesize) {
  if (!path.parent_path().empty() &&
      !std::filesystem::exists(path.parent_path())) {
    std::filesystem::create_directory(path.parent_path());
  }
}

std::string BlobFile::ReadAt(size_t offset, size_t length) const {
  return cache_.ReadAt(offset, length);
}

Cache::Locks BlobFile::ReadAt(size_t offset, std::string_view& out) const {
  int32_t key_size = 0;
  cache_.Copy(&key_size, offset, sizeof(key_size));
  key_size = be32toh(key_size);
  return cache_.ReadAt(offset + sizeof(int32_t), key_size, out);
}

lsn_t BlobFile::Append(std::string_view payload) {
  std::scoped_lock<std::mutex> lk(writer_lock_);
  size_t before = file_writer_.BufferedLSN();
  lsn_t lsn = file_writer_.AddLog(payload);
  cache_.Invalidate(before, payload.length());
  return lsn;
}

}  // namespace tinylamb
