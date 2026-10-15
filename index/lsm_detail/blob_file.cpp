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
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <new>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/crc32c.hpp"
#include "common/serdes.hpp"

namespace tinylamb {
namespace {

constexpr uint32_t kSegmentMetadataMagic = 0x57414c47U;
constexpr uint32_t kSegmentMetadataVersion = 1U;
constexpr size_t kSegmentMetadataSize = 20U;
constexpr uint32_t kMigrationMagic = 0x424c4f4dU;
constexpr uint32_t kMigrationVersion = 1U;
constexpr uint32_t kMigrationCopying = 1U;
constexpr uint32_t kMigrationComplete = 2U;
constexpr size_t kMigrationSize = 40U;

Status IoError(const std::string& action, const std::string& path) {
  return StatusError(StatusCode::kIOError,
                     action + " " + path + ": " + std::strerror(errno));
}

Status Corrupt(const std::string& message) {
  return StatusError(StatusCode::kCorrupt, message);
}

StatusOr<int> OpenAppend(const std::filesystem::path& path) {
  std::error_code ec;
  if (path.has_parent_path()) {
    std::filesystem::create_directories(path.parent_path(), ec);
    if (ec) {
      return StatusError(StatusCode::kIOError,
                         "failed to create blob directory " +
                             path.parent_path().string() + ": " + ec.message());
    }
  }
  const int fd =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0666);
  if (fd < 0) {
    return IoError("failed to open blob", path.string());
  }
  return fd;
}

Status FsyncRetry(int fd) {
  int rc = 0;
  int saved = 0;
  do {
    rc = ::fsync(fd);
    saved = errno;
  } while (rc != 0 && saved == EINTR);
  if (rc != 0) {
    return StatusError(
        StatusCode::kIOError,
        "failed to fsync blob: " + std::string(std::strerror(saved)));
  }
  return Status::kSuccess;
}

Status FdataSyncRetry(int fd) {
  int rc = 0;
  int saved = 0;
  do {
    rc = ::fdatasync(fd);
    saved = errno;
  } while (rc != 0 && saved == EINTR);
  if (rc != 0) {
    return StatusError(
        StatusCode::kIOError,
        "failed to fdatasync blob: " + std::string(std::strerror(saved)));
  }
  return Status::kSuccess;
}

Status SyncDirectoryOf(const std::filesystem::path& path) {
  const std::filesystem::path dir =
      path.has_parent_path() ? path.parent_path() : std::filesystem::path(".");
  const int fd = ::open(dir.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECTORY);
  if (fd < 0) {
    return IoError("failed to open directory for sync", dir.string());
  }
  int rc = 0;
  int saved = 0;
  do {
    rc = ::fsync(fd);
    saved = errno;
  } while (rc != 0 && saved == EINTR);
  const int close_rc = ::close(fd);
  if (rc != 0) {
    return StatusError(StatusCode::kIOError, "failed to sync directory " +
                                                 dir.string() + ": " +
                                                 std::strerror(saved));
  }
  if (close_rc != 0) {
    return IoError("failed to close directory", dir.string());
  }
  return Status::kSuccess;
}

Status RemoveIfExists(const std::filesystem::path& path) {
  std::error_code ec;
  std::filesystem::remove(path, ec);
  if (ec) {
    return StatusError(
        StatusCode::kIOError,
        "failed to remove " + path.string() + ": " + ec.message());
  }
  return Status::kSuccess;
}

StatusOr<uint64_t> FileSize(const std::filesystem::path& path) {
  std::error_code ec;
  const uintmax_t size = std::filesystem::file_size(path, ec);
  if (ec) {
    return StatusError(StatusCode::kIOError,
                       "failed to stat " + path.string() + ": " + ec.message());
  }
  if (size > std::numeric_limits<uint64_t>::max()) {
    return Corrupt("blob file is too large: " + path.string());
  }
  return static_cast<uint64_t>(size);
}

Status ReadBytes(int fd, char* data, size_t length) {
  size_t done = 0;
  while (done < length) {
    const ssize_t n = ::read(fd, data + done, length - done);
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n <= 0) {
      return StatusError(StatusCode::kCorrupt,
                         "unexpected end of blob metadata");
    }
    done += static_cast<size_t>(n);
  }
  return Status::kSuccess;
}

Status WriteBytes(int fd, std::string_view data) {
  size_t done = 0;
  while (done < data.size()) {
    const ssize_t n = ::write(fd, data.data() + done, data.size() - done);
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n <= 0) {
      return IoError("failed to write blob migration state", "");
    }
    done += static_cast<size_t>(n);
  }
  return Status::kSuccess;
}

StatusOr<size_t> ReadSegmentSize(const std::filesystem::path& path,
                                 bool* exists) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    const int saved = errno;
    if (saved == ENOENT) {
      *exists = false;
      return size_t{0};
    }
    return IoError("failed to read blob segment metadata", path.string());
  }
  std::array<char, kSegmentMetadataSize> encoded{};
  const Status status = ReadBytes(fd, encoded.data(), encoded.size());
  const int close_rc = ::close(fd);
  if (status != Status::kSuccess) {
    return status;
  }
  if (close_rc != 0) {
    return IoError("failed to close blob segment metadata", path.string());
  }
  uint32_t magic = 0;
  uint32_t version = 0;
  uint64_t segment_size = 0;
  uint32_t stored_crc = 0;
  DeserializeU32(encoded.data(), &magic);
  DeserializeU32(encoded.data() + sizeof(uint32_t), &version);
  DeserializeU64(encoded.data() + (sizeof(uint32_t) * 2), &segment_size);
  DeserializeU32(encoded.data() + kSegmentMetadataSize - sizeof(uint32_t),
                 &stored_crc);
  if (magic != kSegmentMetadataMagic || version != kSegmentMetadataVersion ||
      segment_size == 0 || segment_size > std::numeric_limits<size_t>::max() ||
      stored_crc !=
          Crc32C(encoded.data(), kSegmentMetadataSize - sizeof(uint32_t))) {
    return Corrupt("invalid blob segment metadata: " + path.string());
  }
  *exists = true;
  return static_cast<size_t>(segment_size);
}

std::filesystem::path MigrationPath(const std::filesystem::path& base) {
  return base.string() + ".migrating";
}

std::filesystem::path MigrationTempPath(const std::filesystem::path& base) {
  return MigrationPath(base).string() + ".tmp";
}

struct MigrationMarker {
  uint32_t state{0};
  uint64_t segment_size{0};
  uint64_t total_size{0};
  uint64_t highest{0};
};

StatusOr<MigrationMarker> ReadMigration(const std::filesystem::path& path,
                                        bool* exists) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    const int saved = errno;
    if (saved == ENOENT) {
      *exists = false;
      return MigrationMarker{};
    }
    return IoError("failed to read blob migration state", path.string());
  }
  std::array<char, kMigrationSize> encoded{};
  const Status status = ReadBytes(fd, encoded.data(), encoded.size());
  const int close_rc = ::close(fd);
  if (status != Status::kSuccess) {
    return status;
  }
  if (close_rc != 0) {
    return IoError("failed to close blob migration state", path.string());
  }
  uint32_t magic = 0;
  uint32_t version = 0;
  uint32_t state = 0;
  uint64_t segment_size = 0;
  uint64_t total_size = 0;
  uint64_t highest = 0;
  uint32_t stored_crc = 0;
  DeserializeU32(encoded.data(), &magic);
  DeserializeU32(encoded.data() + sizeof(uint32_t), &version);
  DeserializeU32(encoded.data() + (sizeof(uint32_t) * 2), &state);
  DeserializeU64(encoded.data() + (sizeof(uint32_t) * 3), &segment_size);
  DeserializeU64(encoded.data() + (sizeof(uint32_t) * 3) + sizeof(uint64_t),
                 &total_size);
  DeserializeU64(
      encoded.data() + (sizeof(uint32_t) * 3) + (sizeof(uint64_t) * 2),
      &highest);
  DeserializeU32(encoded.data() + kMigrationSize - sizeof(uint32_t),
                 &stored_crc);
  if (magic != kMigrationMagic || version != kMigrationVersion ||
      (state != kMigrationCopying && state != kMigrationComplete) ||
      segment_size == 0 || segment_size > std::numeric_limits<size_t>::max() ||
      stored_crc != Crc32C(encoded.data(), kMigrationSize - sizeof(uint32_t))) {
    return Corrupt("invalid blob migration state: " + path.string());
  }
  if (highest > 0 &&
      segment_size > std::numeric_limits<uint64_t>::max() / highest) {
    return Corrupt("blob migration offsets overflow: " + path.string());
  }
  const uint64_t lower = segment_size * highest;
  if (total_size < lower || total_size - lower > segment_size) {
    return Corrupt("invalid blob migration extent: " + path.string());
  }
  *exists = true;
  return MigrationMarker{.state = state,
                         .segment_size = segment_size,
                         .total_size = total_size,
                         .highest = highest};
}

Status WriteMigration(const std::filesystem::path& base, uint32_t state,
                      uint64_t segment_size, uint64_t total_size,
                      uint64_t highest) {
  const std::filesystem::path path = MigrationPath(base);
  const std::filesystem::path tmp = MigrationTempPath(base);
  std::array<char, kMigrationSize> encoded{};
  SerializeU32(encoded.data(), kMigrationMagic);
  SerializeU32(encoded.data() + sizeof(uint32_t), kMigrationVersion);
  SerializeU32(encoded.data() + (sizeof(uint32_t) * 2), state);
  SerializeU64(encoded.data() + (sizeof(uint32_t) * 3), segment_size);
  SerializeU64(encoded.data() + (sizeof(uint32_t) * 3) + sizeof(uint64_t),
               total_size);
  SerializeU64(encoded.data() + (sizeof(uint32_t) * 3) + (sizeof(uint64_t) * 2),
               highest);
  const uint32_t crc =
      Crc32C(encoded.data(), kMigrationSize - sizeof(uint32_t));
  SerializeU32(encoded.data() + kMigrationSize - sizeof(uint32_t), crc);

  const int fd =
      ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0666);
  if (fd < 0) {
    return IoError("failed to create blob migration state", tmp.string());
  }
  Status status =
      WriteBytes(fd, std::string_view(encoded.data(), encoded.size()));
  if (status == Status::kSuccess) {
    status = FsyncRetry(fd);
  }
  const int close_rc = ::close(fd);
  if (status != Status::kSuccess) {
    std::error_code remove_ec;
    std::filesystem::remove(tmp, remove_ec);
    return status;
  }
  if (close_rc != 0) {
    std::error_code remove_ec;
    std::filesystem::remove(tmp, remove_ec);
    return IoError("failed to close blob migration state", tmp.string());
  }
  std::error_code ec;
  std::filesystem::rename(tmp, path, ec);
  if (ec) {
    std::error_code remove_ec;
    std::filesystem::remove(tmp, remove_ec);
    return StatusError(StatusCode::kIOError,
                       "failed to publish blob migration state " +
                           path.string() + ": " + ec.message());
  }
  return SyncDirectoryOf(path);
}

bool ParseNumericSegment(const std::string& name,
                         const std::filesystem::path& base, uint64_t* index) {
  const std::string prefix = base.filename().string() + ".";
  if (!name.starts_with(prefix)) {
    return false;
  }
  const std::string suffix = name.substr(prefix.size());
  if (suffix.empty()) {
    return false;
  }
  uint64_t parsed = 0;
  for (const char digit : suffix) {
    if (digit < '0' || digit > '9') {
      return false;
    }
    const auto value = static_cast<uint64_t>(digit - '0');
    if (parsed > (std::numeric_limits<uint64_t>::max() - value) / 10) {
      return false;
    }
    parsed = (parsed * 10) + value;
  }
  *index = parsed;
  return true;
}

struct NumericSegments {
  bool any{false};
  uint64_t highest{0};
  std::map<uint64_t, uint64_t> sizes;
  std::map<uint64_t, std::filesystem::path> paths;
};

StatusOr<NumericSegments> DiscoverNumericSegments(
    const std::filesystem::path& base) {
  NumericSegments result;
  const std::filesystem::path dir =
      base.has_parent_path() ? base.parent_path() : std::filesystem::path(".");
  std::error_code ec;
  std::filesystem::directory_iterator it(dir, ec);
  if (ec) {
    return StatusError(
        StatusCode::kIOError,
        "failed to scan blob directory " + dir.string() + ": " + ec.message());
  }
  for (const auto& entry : it) {
    uint64_t index = 0;
    if (!ParseNumericSegment(entry.path().filename().string(), base, &index)) {
      continue;
    }
    if (ec) {
      return StatusError(StatusCode::kIOError,
                         "failed to stat old blob segment " +
                             entry.path().string() + ": " + ec.message());
    }
    if (!entry.is_regular_file(ec)) {
      return Corrupt("old blob segment is not a regular file: " +
                     entry.path().string());
    }
    if (ec) {
      return StatusError(StatusCode::kIOError,
                         "failed to stat old blob segment " +
                             entry.path().string() + ": " + ec.message());
    }
    if (index == 0) {
      return Corrupt("blob has a numeric segment zero: " +
                     entry.path().string());
    }
    ASSIGN_OR_RETURN(uint64_t, size, FileSize(entry.path()));
    if (!result.sizes.emplace(index, size).second) {
      return Corrupt("duplicate blob segment: " + entry.path().string());
    }
    result.paths.emplace(index, entry.path());
    result.any = true;
    result.highest = std::max(result.highest, index);
  }
  return result;
}

using SegmentExtent = std::pair<uint64_t, uint64_t>;

StatusOr<SegmentExtent> ValidateSeries(const std::filesystem::path& base,
                                       uint64_t segment_size,
                                       const NumericSegments& segments,
                                       bool allow_partial_base) {
  std::error_code ec;
  const bool base_exists = std::filesystem::exists(base, ec);
  if (ec) {
    return StatusError(
        StatusCode::kIOError,
        "failed to stat blob " + base.string() + ": " + ec.message());
  }
  if (!base_exists) {
    return Corrupt("blob base is missing: " + base.string());
  }
  ASSIGN_OR_RETURN(uint64_t, base_size, FileSize(base));
  if (!segments.any) {
    if (base_size > segment_size) {
      return Corrupt("blob base is larger than its segment: " + base.string());
    }
    return SegmentExtent(0, base_size);
  }
  if (!allow_partial_base && base_size != segment_size) {
    return Corrupt("blob segment series has a gap after the base: " +
                   base.string());
  }
  for (uint64_t index = 1; index < segments.highest; ++index) {
    const auto found = segments.sizes.find(index);
    if (found == segments.sizes.end() || found->second != segment_size) {
      return Corrupt("blob segment series is missing a segment");
    }
  }
  const auto highest = segments.sizes.find(segments.highest);
  if (highest == segments.sizes.end() || highest->second > segment_size) {
    return Corrupt("blob segment series has an invalid tail segment");
  }
  if (segments.highest > std::numeric_limits<uint64_t>::max() / segment_size) {
    return Corrupt("blob segment series offset overflows");
  }
  const uint64_t total = (segments.highest * segment_size) + highest->second;
  if (base_size > total) {
    return Corrupt("blob base extends beyond its segment series");
  }
  return SegmentExtent(segments.highest, total);
}

Status CopySegments(const std::filesystem::path& base, uint64_t segment_size,
                    const NumericSegments& segments) {
  const int base_fd = ::open(base.c_str(), O_RDWR | O_CLOEXEC);
  if (base_fd < 0) {
    return IoError("failed to open blob base for migration", base.string());
  }
  std::vector<char> chunk(1U << 20);
  for (const auto& [index, size] : segments.sizes) {
    if (index > std::numeric_limits<uint64_t>::max() / segment_size) {
      ::close(base_fd);
      return Corrupt("blob segment offset overflows");
    }
    const uint64_t absolute = index * segment_size;
    if (absolute > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
      ::close(base_fd);
      return Corrupt("blob segment offset does not fit off_t");
    }
    const int source_fd =
        ::open(segments.paths.at(index).c_str(), O_RDONLY | O_CLOEXEC);
    if (source_fd < 0) {
      ::close(base_fd);
      return IoError("failed to open old blob segment",
                     segments.paths.at(index).string());
    }
    size_t done = 0;
    while (done < size) {
      const size_t want =
          std::min(chunk.size(), static_cast<size_t>(size - done));
      ssize_t read_count = 0;
      do {
        read_count =
            ::pread(source_fd, chunk.data(), want, static_cast<off_t>(done));
      } while (read_count < 0 && errno == EINTR);
      if (read_count <= 0) {
        const int saved = errno;
        ::close(source_fd);
        ::close(base_fd);
        return StatusError(StatusCode::kIOError,
                           "failed to read old blob segment " +
                               segments.paths.at(index).string() + ": " +
                               std::strerror(read_count == 0 ? EIO : saved));
      }
      size_t written = 0;
      while (std::cmp_less(written, read_count)) {
        const ssize_t write_count =
            ::pwrite(base_fd, chunk.data() + written,
                     static_cast<size_t>(read_count) - written,
                     static_cast<off_t>(absolute + done + written));
        if (write_count < 0 && errno == EINTR) {
          continue;
        }
        if (write_count <= 0) {
          const int saved = errno;
          ::close(source_fd);
          ::close(base_fd);
          return StatusError(StatusCode::kIOError,
                             "failed to merge old blob segment " +
                                 segments.paths.at(index).string() + ": " +
                                 std::strerror(write_count == 0 ? EIO : saved));
        }
        written += static_cast<size_t>(write_count);
      }
      done += static_cast<size_t>(read_count);
    }
    ::close(source_fd);
  }
  Status sync_status = FsyncRetry(base_fd);
  const int close_rc = ::close(base_fd);
  if (sync_status != Status::kSuccess) {
    return sync_status;
  }
  if (close_rc != 0) {
    return IoError("failed to close migrated blob", base.string());
  }
  return Status::kSuccess;
}

Status FinishMigration(const std::filesystem::path& base,
                       const std::filesystem::path& metadata,
                       const MigrationMarker& marker) {
  ASSIGN_OR_RETURN(NumericSegments, remaining, DiscoverNumericSegments(base));
  for (const auto& [index, size] : remaining.sizes) {
    if (index == 0 || index > marker.highest || size > marker.segment_size) {
      return Corrupt("unexpected blob segment after migration");
    }
  }
  for (const auto& [index, size] : remaining.sizes) {
    (void)size;
    RETURN_IF_FAIL(RemoveIfExists(remaining.paths.at(index)));
  }
  RETURN_IF_FAIL(RemoveIfExists(metadata));
  RETURN_IF_FAIL(SyncDirectoryOf(base));
  RETURN_IF_FAIL(RemoveIfExists(MigrationPath(base)));
  return SyncDirectoryOf(base);
}

Status MigrateLegacyBlob(const std::filesystem::path& base) {
  const std::filesystem::path metadata = base.string() + ".meta";
  const std::filesystem::path migration = MigrationPath(base);
  bool migration_exists = false;
  ASSIGN_OR_RETURN(MigrationMarker, marker,
                   ReadMigration(migration, &migration_exists));
  ASSIGN_OR_RETURN(NumericSegments, segments, DiscoverNumericSegments(base));

  if (!segments.any) {
    if (migration_exists) {
      if (marker.state != kMigrationComplete) {
        return Corrupt("incomplete blob migration has no source segments");
      }
      ASSIGN_OR_RETURN(uint64_t, base_size, FileSize(base));
      if (base_size < marker.total_size) {
        return Corrupt("completed blob migration is shorter than its extent");
      }
      RETURN_IF_FAIL(RemoveIfExists(metadata));
      RETURN_IF_FAIL(RemoveIfExists(migration));
      return SyncDirectoryOf(base);
    }
    std::error_code metadata_ec;
    const bool metadata_present =
        std::filesystem::exists(metadata, metadata_ec);
    if (metadata_ec) {
      return StatusError(
          StatusCode::kIOError,
          "failed to stat obsolete blob metadata: " + metadata_ec.message());
    }
    if (metadata_present) {
      RETURN_IF_FAIL(RemoveIfExists(metadata));
      return SyncDirectoryOf(base);
    }
    return Status::kSuccess;
  }

  bool metadata_exists = false;
  ASSIGN_OR_RETURN(size_t, segment_size,
                   ReadSegmentSize(metadata, &metadata_exists));
  if (!metadata_exists) {
    return Corrupt("segmented blob has no geometry metadata: " + base.string());
  }
  if (migration_exists && marker.segment_size != segment_size) {
    return Corrupt("blob migration segment size changed");
  }

  uint64_t expected_highest = 0;
  uint64_t total_size = 0;
  if (migration_exists && marker.state == kMigrationComplete) {
    if (segments.highest > marker.highest) {
      return Corrupt("blob has segments beyond its migration extent");
    }
    // Both branches assign expected_highest before its single use below; the
    // analyzer cannot join the paths and reports this store as dead. The
    // NOLINT stays on the store's anchor line.
    expected_highest =  // NOLINT(clang-analyzer-deadcode.DeadStores)
        marker.highest;
    total_size = marker.total_size;
    ASSIGN_OR_RETURN(uint64_t, base_size, FileSize(base));
    if (base_size < total_size) {
      return Corrupt("completed blob migration is shorter than its extent");
    }
  } else {
    ASSIGN_OR_RETURN(
        SegmentExtent, extent,
        ValidateSeries(base, segment_size, segments, migration_exists));
    expected_highest = extent.first;
    total_size = extent.second;
    if (migration_exists && (marker.highest != expected_highest ||
                             marker.total_size != total_size)) {
      return Corrupt("incomplete blob migration extent changed");
    }
    if (!migration_exists) {
      RETURN_IF_FAIL(WriteMigration(base, kMigrationCopying, segment_size,
                                    total_size, expected_highest));
    }
    RETURN_IF_FAIL(CopySegments(base, segment_size, segments));
    RETURN_IF_FAIL(WriteMigration(base, kMigrationComplete, segment_size,
                                  total_size, expected_highest));
    marker = MigrationMarker{.state = kMigrationComplete,
                             .segment_size = segment_size,
                             .total_size = total_size,
                             .highest = expected_highest};
  }

  return FinishMigration(base, metadata, marker);
}

}  // namespace

StatusOr<std::unique_ptr<BlobFile>> BlobFile::Create(
    const std::filesystem::path& path, size_t memory_capacity,
    size_t max_filesize) {
  if (memory_capacity == 0) {
    return StatusError(StatusCode::kInvalidArgument,
                       "BlobFile cache size is zero");
  }
  std::error_code ec;
  if (path.has_parent_path()) {
    std::filesystem::create_directories(path.parent_path(), ec);
    if (ec) {
      return StatusError(StatusCode::kIOError,
                         "failed to create blob directory " +
                             path.parent_path().string() + ": " + ec.message());
    }
  }
  RETURN_IF_FAIL(MigrateLegacyBlob(path));
  ASSIGN_OR_RETURN(int, fd, OpenAppend(path));
  const Status directory_status = SyncDirectoryOf(path);
  if (directory_status != Status::kSuccess) {
    ::close(fd);
    return directory_status;
  }
  struct stat stat_buffer{};
  if (::fstat(fd, &stat_buffer) != 0) {
    const int saved = errno;
    ::close(fd);
    return StatusError(
        StatusCode::kIOError,
        "failed to stat blob " + path.string() + ": " + std::strerror(saved));
  }
  if (stat_buffer.st_size < 0 || static_cast<uintmax_t>(stat_buffer.st_size) >
                                     std::numeric_limits<uint64_t>::max()) {
    ::close(fd);
    return Corrupt("invalid blob file size: " + path.string());
  }
  const auto initial_size = static_cast<uint64_t>(stat_buffer.st_size);
  if (max_filesize != 0 && initial_size > max_filesize) {
    ::close(fd);
    return StatusError(StatusCode::kTooBigData,
                       "blob file exceeds its configured maximum");
  }
  const size_t cache_limit =
      max_filesize == 0
          ? static_cast<size_t>(std::max<uint64_t>(initial_size, 4096))
          : max_filesize;
  StatusOr<std::unique_ptr<Cache>> cache_result =
      Cache::Create(fd, memory_capacity, cache_limit);
  if (!cache_result.HasValue()) {
    ::close(fd);
    return cache_result.GetStatus();
  }
  std::unique_ptr<Cache> cache = cache_result.MoveValue();
  try {
    return std::unique_ptr<BlobFile>(new BlobFile(
        fd, static_cast<lsn_t>(initial_size), max_filesize, std::move(cache)));
  } catch (const std::bad_alloc&) {
    ::close(fd);
    return StatusError(StatusCode::kIOError, "failed to allocate BlobFile");
  }
}

BlobFile::BlobFile(int fd, lsn_t written, size_t max_filesize,
                   std::unique_ptr<Cache> cache)
    : fd_(fd),
      written_(written),
      max_filesize_(max_filesize),
      cache_(std::move(cache)) {}

BlobFile::~BlobFile() {
  cache_.reset();
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

Status BlobFile::Flush() const {
  std::scoped_lock lock(writer_lock_);
  if (fd_ < 0) {
    return StatusError(StatusCode::kIOError, "blob file is closed");
  }
  return Status::kSuccess;
}

Status BlobFile::Sync() {
  std::scoped_lock lock(writer_lock_);
  if (fd_ < 0) {
    return StatusError(StatusCode::kIOError, "blob file is closed");
  }
  return FdataSyncRetry(fd_);
}

StatusOr<std::string> BlobFile::ReadAt(size_t offset, size_t length) const {
  std::scoped_lock lock(writer_lock_);
  const lsn_t written = written_.load(std::memory_order_relaxed);
  if (offset > written || length > written - offset ||
      (max_filesize_ != 0 &&
       (offset > max_filesize_ || length > max_filesize_ - offset))) {
    return StatusError(StatusCode::kCorrupt,
                       "blob read is outside the written file");
  }
  ASSIGN_OR_RETURN(std::string, result, cache_->ReadAt(offset, length));
  if (result.size() != length) {
    return StatusError(StatusCode::kCorrupt,
                       "blob cache returned a short read");
  }
  return result;
}

StatusOr<Cache::Locks> BlobFile::ReadAt(size_t offset,
                                        std::string_view& out) const {
  std::scoped_lock lock(writer_lock_);
  const lsn_t written = written_.load(std::memory_order_relaxed);
  if (offset > written || written - offset < sizeof(int32_t)) {
    return StatusError(StatusCode::kCorrupt,
                       "blob length header is outside the written file");
  }
  int32_t encoded_size = 0;
  RETURN_IF_FAIL(cache_->Copy(&encoded_size, offset, sizeof(int32_t)));
  const uint32_t unsigned_size = be32toh(static_cast<uint32_t>(encoded_size));
  if (unsigned_size > std::numeric_limits<int32_t>::max()) {
    return StatusError(StatusCode::kCorrupt, "blob entry length is invalid");
  }
  if (offset > std::numeric_limits<size_t>::max() - sizeof(int32_t)) {
    return StatusError(StatusCode::kCorrupt,
                       "blob length header offset overflows");
  }
  const size_t payload_offset = offset + sizeof(int32_t);
  const auto payload_size = static_cast<size_t>(unsigned_size);
  if (payload_size > written - payload_offset ||
      (max_filesize_ != 0 && (payload_offset > max_filesize_ ||
                              payload_size > max_filesize_ - payload_offset))) {
    return StatusError(StatusCode::kCorrupt,
                       "blob entry exceeds the written file");
  }
  out = {};
  return cache_->ReadAt(payload_offset, payload_size, out);
}

StatusOr<lsn_t> BlobFile::Append(std::string_view payload) {
  std::scoped_lock lock(writer_lock_);
  if (fd_ < 0) {
    return StatusError(StatusCode::kIOError, "blob file is closed");
  }
  const lsn_t before = written_.load(std::memory_order_relaxed);
  if (max_filesize_ != 0 &&
      (before > max_filesize_ ||
       payload.size() > max_filesize_ - static_cast<size_t>(before))) {
    return StatusError(StatusCode::kTooBigData,
                       "blob append exceeds its configured maximum");
  }
  if (payload.size() >
      std::numeric_limits<lsn_t>::max() - static_cast<lsn_t>(before)) {
    return StatusError(StatusCode::kTooBigData, "blob offset overflows");
  }
  size_t done = 0;
  while (done < payload.size()) {
    const ssize_t written =
        ::write(fd_, payload.data() + done, payload.size() - done);
    if (written < 0 && errno == EINTR) {
      continue;
    }
    if (written <= 0) {
      const int saved = errno == 0 ? EIO : errno;
      if (::ftruncate(fd_, static_cast<off_t>(before)) != 0 ||
          FdataSyncRetry(fd_) != Status::kSuccess) {
        ::close(fd_);
        fd_ = -1;
      }
      return StatusError(
          StatusCode::kIOError,
          "failed to append blob: " + std::string(std::strerror(saved)));
    }
    done += static_cast<size_t>(written);
  }
  written_.store(before + static_cast<lsn_t>(payload.size()),
                 std::memory_order_release);
  cache_->Invalidate(static_cast<size_t>(before), payload.size());
  return before;
}

}  // namespace tinylamb
