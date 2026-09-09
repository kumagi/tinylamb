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
StatusOr<std::unique_ptr<BlobFile>> BlobFile::Create(
    const std::filesystem::path& path, size_t memory_capacity,
    size_t max_filesize) {
  ASSIGN_OR_RETURN(std::unique_ptr<Logger>, writer, Logger::Create(path));
  ASSIGN_OR_RETURN(std::unique_ptr<Cache>, cache,
                   Cache::Create(writer->Fd(), memory_capacity, max_filesize));
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
  return std::unique_ptr<BlobFile>(
      new BlobFile(std::move(writer), std::move(cache)));  // NOLINT
}

BlobFile::BlobFile(std::unique_ptr<Logger> file_writer,
                   std::unique_ptr<Cache> cache)
    : file_writer_(std::move(file_writer)), cache_(std::move(cache)) {}

StatusOr<std::string> BlobFile::ReadAt(size_t offset, size_t length) const {
  // Appends are asynchronous. A reader on the same BlobFile must nevertheless
  // observe every offset already returned by Append().
  RETURN_IF_FAIL(Flush());
  return cache_->ReadAt(offset, length);
}

StatusOr<Cache::Locks> BlobFile::ReadAt(size_t offset,
                                        std::string_view& out) const {
  RETURN_IF_FAIL(Flush());
  out = {};
  constexpr size_t kHeaderSize = sizeof(int32_t);
  int32_t key_size = 0;
  RETURN_IF_FAIL(cache_->Copy(&key_size, offset, kHeaderSize));
  key_size = static_cast<int32_t>(be32toh(static_cast<uint32_t>(key_size)));
  if (key_size < 0) {
    // Disk-derived length is bogus; refuse instead of propagating garbage.
    return StatusError(StatusCode::kCorrupt, "blob entry header is negative");
  }
  return cache_->ReadAt(offset + kHeaderSize, static_cast<size_t>(key_size),
                        out);
}

StatusOr<lsn_t> BlobFile::Append(std::string_view payload) {
  std::scoped_lock<std::mutex> lk(writer_lock_);
  const size_t before = file_writer_->BufferedLSN();
  ASSIGN_OR_RETURN(lsn_t, lsn, file_writer_->AddLog(payload));
  cache_->Invalidate(before, payload.length());
  return lsn;
}
}  // namespace tinylamb
