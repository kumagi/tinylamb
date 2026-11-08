/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#include "recovery/logger.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/crc32c.hpp"
#include "common/log_message.hpp"
#include "common/platform/file_io.hpp"
#include "common/serdes.hpp"

namespace tinylamb {
namespace {

std::mutex sync_hook_mutex;
std::function<int(int)> before_sync_hook;

StatusOr<int> CreateFile(const std::filesystem::path& path) {
  std::error_code ec;
  if (path.has_parent_path()) {
    std::filesystem::create_directories(path.parent_path(), ec);
    if (ec) {
      return StatusError(StatusCode::kIOError,
                         "Failed to create log directory for " + path.string() +
                             ": " + ec.message());
    }
  }
  const int fd = platform::OpenFile(
      path,
      {.mode = platform::OpenMode::kReadWrite, .create = true, .append = true},
      0666);
  if (fd == -1) {
    return StatusError(
        StatusCode::kIOError,
        "Failed to open log file: " + std::string(std::strerror(errno)) +
            " for " + path.string());
  }
  return fd;
}

// fdatasync (F_FULLFSYNC on macOS) with EINTR retry. The caller must check
// the result: silently skipping a sync would break the durability guarantee.
int FdataSync(int fd) { return platform::DataSync(fd); }

Status SyncDirectoryOf(const std::filesystem::path& path) {
  const std::filesystem::path dir =
      path.has_parent_path() ? path.parent_path() : std::filesystem::path(".");
  if (platform::SyncDirectory(dir) != 0) {
    const int saved = errno;
    return StatusError(StatusCode::kIOError,
                       "Failed to fsync directory: " + dir.string() + ": " +
                           std::strerror(saved));
  }
  return Status::kSuccess;
}

constexpr uint32_t kSegmentMetadataMagic = 0x57414c47U;
constexpr uint32_t kSegmentMetadataVersion = 1U;
constexpr size_t kSegmentMetadataSize = 20U;

// Fixed files keep allocation and i_size out of the commit path. The two
// end slots are outside LSN space; both must validate before trusting either.
constexpr size_t kTailSlotBytes = 32;
constexpr size_t kTailBytes = 2 * kTailSlotBytes;
constexpr uint32_t kTailMagic = 0x544c4632U;

struct FixedTail {
  uint64_t generation{0};
  size_t end{0};
};

std::array<char, kTailSlotBytes> EncodeTail(uint64_t index, uint64_t generation,
                                            size_t end) {
  std::array<char, kTailSlotBytes> bytes{};
  SerializeU32(bytes.data(), kTailMagic);
  SerializeU64(bytes.data() + 4, index);
  SerializeU64(bytes.data() + 12, generation);
  SerializeU64(bytes.data() + 20, end);
  SerializeU32(bytes.data() + 28, Crc32C(bytes.data(), 28));
  return bytes;
}

int PwriteAll(int fd, const char* bytes, size_t length, int64_t offset) {
  size_t done = 0;
  while (done < length) {
    const ssize_t written = platform::Pwrite(
        fd, bytes + done, length - done, offset + static_cast<int64_t>(done));
    if (written < 0 && errno == EINTR) {
      continue;
    }
    if (written <= 0) {
      if (written == 0) {
        errno = EIO;
      }
      return -1;
    }
    done += static_cast<size_t>(written);
  }
  return 0;
}

int ClearAppend(int fd) { return platform::ClearAppend(fd); }

int InitializeFixedFile(int fd, uint64_t index, size_t size, size_t end,
                        bool initialize_data = false) {
  if (size > static_cast<size_t>(std::numeric_limits<int64_t>::max()) -
                 kTailBytes ||
      end > size) {
    errno = EOVERFLOW;
    return -1;
  }
  if (ClearAppend(fd) != 0) {
    return -1;
  }
  const auto physical_size = static_cast<int64_t>(size + kTailBytes);
  if (platform::Preallocate(fd, physical_size) != 0) {
    return -1;
  }
  // ftruncate also repairs an explicitly forced overlong frame. It never
  // alters the data prefix [0, size); logical truncation only changes slots.
  if (platform::Truncate(fd, physical_size) != 0) {
    return -1;
  }
  if (initialize_data) {
    // fallocate alone leaves unwritten extents: their first writes still
    // need extent-conversion metadata at commit. Materialize new data blocks
    // once, before any records/ACKs exist. Forced repair and truncation must
    // preserve all existing data bytes and never take this branch.
    const std::array<char, size_t{64} * 1024> zeros{};
    for (size_t offset = 0; offset < size; offset += zeros.size()) {
      const size_t count = std::min(zeros.size(), size - offset);
      if (PwriteAll(fd, zeros.data(), count, static_cast<int64_t>(offset)) !=
          0) {
        return -1;
      }
    }
  }
  const auto bytes = EncodeTail(index, 0, end);
  if (PwriteAll(fd, bytes.data(), bytes.size(), static_cast<int64_t>(size)) !=
          0 ||
      PwriteAll(fd, bytes.data(), bytes.size(),
                static_cast<int64_t>(size + kTailSlotBytes)) != 0) {
    return -1;
  }
  return FdataSync(fd);
}

StatusOr<FixedTail> ReadFixedTail(int fd, uint64_t index, size_t size,
                                  bool allow_torn) {
  int64_t file_size = 0;
  if (platform::FileSize(fd, &file_size) != 0) {
    return StatusError(StatusCode::kIOError, "Failed to stat fixed WAL frame");
  }
  bool valid =
      file_size >= 0 &&
      size <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) -
                  kTailBytes &&
      std::cmp_equal(file_size, size + kTailBytes);
  FixedTail latest;
  std::array<FixedTail, 2> tails{};
  for (size_t slot = 0; valid && slot < 2; ++slot) {
    std::array<char, kTailSlotBytes> bytes{};
    ssize_t count = 0;
    do {
      count =
          platform::Pread(fd, bytes.data(), bytes.size(),
                          static_cast<int64_t>(size + (slot * kTailSlotBytes)));
    } while (count < 0 && errno == EINTR);
    if (count < 0) {
      return StatusError(StatusCode::kIOError,
                         "Failed to read fixed WAL end slot");
    }
    uint32_t magic = 0;
    uint32_t crc = 0;
    uint64_t stored_index = 0;
    uint64_t generation = 0;
    uint64_t end = 0;
    DeserializeU32(bytes.data(), &magic);
    DeserializeU64(bytes.data() + 4, &stored_index);
    DeserializeU64(bytes.data() + 12, &generation);
    DeserializeU64(bytes.data() + 20, &end);
    DeserializeU32(bytes.data() + 28, &crc);
    valid = std::cmp_equal(count, bytes.size()) && magic == kTailMagic &&
            stored_index == index && end <= size &&
            crc == Crc32C(bytes.data(), 28);
    tails[slot] = {.generation = generation, .end = static_cast<size_t>(end)};
  }
  if (valid) {
    const auto& a = tails[0];
    const auto& b = tails[1];
    // A reset has two identical generation-zero slots. Normal publication
    // alternates adjacent generations and never moves the end backwards.
    valid = (a.generation == 0 && b.generation == 0 && a.end == b.end) ||
            (a.generation > b.generation && a.generation - b.generation == 1 &&
             a.end >= b.end && a.generation % 2 == 0) ||
            (b.generation > a.generation && b.generation - a.generation == 1 &&
             b.end >= a.end && b.generation % 2 == 1);
    latest = a.generation >= b.generation ? a : b;
  }
  if (!valid) {
    if (!allow_torn) {
      return StatusError(StatusCode::kCorrupt,
                         "Fixed WAL logical-end slots/length are corrupt");
    }
    // --force must inspect all available record bytes, not silently trust an
    // older slot which might precede an acknowledged commit. Recovery finds
    // the CRC-valid prefix and performs the actual logical truncation later.
    latest = {.generation = 0,
              .end = file_size <= 0
                         ? 0
                         : static_cast<size_t>(std::min<uint64_t>(
                               static_cast<uint64_t>(file_size), size))};
  }
  return latest;
}

// Cap for the flush worker's adaptive idle sleep. An idle worker (nothing
// buffered, no durability waiters) otherwise re-arms a 1 ms timed wait a
// thousand times per second for as long as the database sits quiet. The
// backoff doubles from sync_interval_ up to this cap and resets on any work;
// its only cost is the stale-worker_sleeping_ window in AddLog, which delays
// an ASYNC record's flush by at most one idle interval (synchronous commits
// always NotifyWorker, so their durability latency is unaffected) -- the same
// trade a classic wal-writer delay makes.
constexpr std::chrono::milliseconds kIdleWaitCap{100};

}  // namespace

std::filesystem::path Logger::SegmentMetadataPath(
    const std::filesystem::path& base) {
  return base.string() + ".meta";
}

StatusOr<size_t> Logger::ReadSegmentMetadata(const std::filesystem::path& base,
                                             bool* exists, bool* fixed_size) {
  const std::filesystem::path path = SegmentMetadataPath(base);
  std::error_code ec;
  const bool present = std::filesystem::exists(path, ec);
  if (ec) {
    return StatusError(StatusCode::kIOError,
                       "Failed to stat WAL segment metadata " + path.string() +
                           ": " + ec.message());
  }
  *exists = present;
  if (fixed_size != nullptr) {
    *fixed_size = false;
  }
  if (!present) {
    return size_t{0};
  }
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return StatusError(StatusCode::kCorrupt,
                       "Failed to read WAL segment metadata " + path.string());
  }
  std::array<char, kSegmentMetadataSize> encoded{};
  in.read(encoded.data(), static_cast<std::streamsize>(encoded.size()));
  if (in.gcount() != static_cast<std::streamsize>(encoded.size())) {
    return StatusError(StatusCode::kCorrupt,
                       "WAL segment metadata is truncated: " + path.string());
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
  const uint32_t computed =
      Crc32C(encoded.data(), kSegmentMetadataSize - sizeof(uint32_t));
  if (magic != kSegmentMetadataMagic ||
      (version != kSegmentMetadataVersion && version != 2U) ||
      segment_size == 0 || stored_crc != computed) {
    return StatusError(
        StatusCode::kCorrupt,
        "WAL segment metadata checksum/version mismatch: " + path.string());
  }
  if (segment_size > std::numeric_limits<size_t>::max()) {
    return StatusError(
        StatusCode::kCorrupt,
        "WAL segment size does not fit this platform: " + path.string());
  }
  if (fixed_size != nullptr) {
    *fixed_size = version == 2U;
  }
  return static_cast<size_t>(segment_size);
}

Status Logger::WriteSegmentMetadata(const std::filesystem::path& base,
                                    size_t segment_size, bool fixed_size) {
  if (segment_size == 0) {
    return StatusError(StatusCode::kIOError, "WAL segment size must be > 0");
  }
  const std::filesystem::path path = SegmentMetadataPath(base);
  std::error_code ec;
  if (path.has_parent_path()) {
    std::filesystem::create_directories(path.parent_path(), ec);
    if (ec) {
      return StatusError(StatusCode::kIOError,
                         "Failed to create WAL metadata directory " +
                             path.parent_path().string() + ": " + ec.message());
    }
  }
  const std::filesystem::path tmp = path.string() + ".tmp";
  std::array<char, kSegmentMetadataSize> encoded{};
  SerializeU32(encoded.data(), kSegmentMetadataMagic);
  SerializeU32(encoded.data() + sizeof(uint32_t),
               fixed_size ? 2U : kSegmentMetadataVersion);
  SerializeU64(encoded.data() + (sizeof(uint32_t) * 2), segment_size);
  const uint32_t crc =
      Crc32C(encoded.data(), kSegmentMetadataSize - sizeof(uint32_t));
  SerializeU32(encoded.data() + kSegmentMetadataSize - sizeof(uint32_t), crc);
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) {
      return StatusError(StatusCode::kIOError,
                         "Failed to open WAL segment metadata " + tmp.string());
    }
    out.write(encoded.data(), static_cast<std::streamsize>(encoded.size()));
    out.flush();
    if (!out) {
      std::error_code remove_ec;
      std::filesystem::remove(tmp, remove_ec);
      return StatusError(
          StatusCode::kIOError,
          "Failed to write WAL segment metadata " + tmp.string());
    }
  }
  const int fd =
      platform::OpenFile(tmp, {.mode = platform::OpenMode::kReadOnly});
  if (fd < 0 || FdataSync(fd) != 0) {
    const int saved = errno;
    if (fd >= 0) {
      platform::CloseFile(fd);
    }
    std::error_code remove_ec;
    std::filesystem::remove(tmp, remove_ec);
    return StatusError(StatusCode::kIOError,
                       "Failed to fsync WAL segment metadata " + tmp.string() +
                           ": " + std::strerror(saved));
  }
  const int close_rc = platform::CloseFile(fd);
  if (close_rc != 0) {
    const int saved = errno;
    std::error_code remove_ec;
    std::filesystem::remove(tmp, remove_ec);
    return StatusError(StatusCode::kIOError,
                       "Failed to close WAL segment metadata " + tmp.string() +
                           ": " + std::strerror(saved));
  }
  std::filesystem::rename(tmp, path, ec);
  if (ec) {
    std::error_code remove_ec;
    std::filesystem::remove(tmp, remove_ec);
    return StatusError(StatusCode::kIOError,
                       "Failed to rename WAL segment metadata " +
                           path.string() + ": " + ec.message());
  }
  return SyncDirectoryOf(path);
}

StatusOr<bool> Logger::FixedSizeSegmentFormat(
    const std::filesystem::path& base) {
  bool exists = false;
  bool fixed = false;
  ASSIGN_OR_RETURN(size_t, unused, ReadSegmentMetadata(base, &exists, &fixed));
  (void)unused;
  return fixed;
}

StatusOr<size_t> Logger::SegmentLogicalSize(const std::filesystem::path& base,
                                            uint64_t index, size_t segment_size,
                                            bool allow_torn_footer) {
  bool exists = false;
  bool fixed = false;
  ASSIGN_OR_RETURN(size_t, persisted,
                   ReadSegmentMetadata(base, &exists, &fixed));
  if (fixed && persisted != segment_size) {
    return StatusError(StatusCode::kCorrupt, "Fixed WAL geometry mismatch");
  }
  const auto path = SegmentPath(base, index);
  if (!fixed) {
    std::error_code ec;
    const auto size = std::filesystem::file_size(path, ec);
    if (ec || size > std::numeric_limits<size_t>::max()) {
      return StatusError(StatusCode::kIOError,
                         "Failed to stat WAL " + path.string());
    }
    return static_cast<size_t>(size);
  }
  const int fd =
      platform::OpenFile(path, {.mode = platform::OpenMode::kReadOnly});
  if (fd < 0) {
    return StatusError(StatusCode::kIOError,
                       "Failed to read fixed WAL " + path.string());
  }
  auto tail = ReadFixedTail(fd, index, segment_size, allow_torn_footer);
  platform::CloseFile(fd);
  if (!tail.HasValue()) {
    return tail.GetStatus();
  }
  return tail.Value().end;
}

namespace {

// True when the file name ends with `.` + a fully numeric suffix, i.e. it is
// one of this logger's segment files rather than an unrelated neighbor.
bool ParseSegmentSuffix(const std::string& name, uint64_t* index) {
  static constexpr std::string_view kNumeric = "0123456789";
  const size_t dot = name.rfind('.');
  if (dot == std::string::npos || dot + 2 > name.size()) {
    return false;
  }
  const std::string_view suffix(name.data() + dot + 1, name.size() - dot - 1);
  if (suffix.empty() ||
      suffix.find_first_not_of(kNumeric) != std::string_view::npos) {
    return false;
  }
  uint64_t parsed = 0;
  for (const char digit : suffix) {
    const auto value = static_cast<uint64_t>(digit - '0');
    if (parsed > ((std::numeric_limits<uint64_t>::max() - value) / 10)) {
      return false;
    }
    parsed = (parsed * 10) + value;
  }
  *index = parsed;
  return true;
}

}  // namespace

std::filesystem::path Logger::SegmentPath(const std::filesystem::path& base,
                                          uint64_t index) {
  if (index == 0) {
    // Segment 0 is the legacy single-file WAL: the base path itself.  This
    // keeps databases created before segmentation (and every test harness
    // that opens <db>.log) readable without a migration step.
    return base;
  }
  static constexpr size_t kIndexDigits = 20;
  std::string suffix = std::to_string(index);
  if (suffix.size() < kIndexDigits) {
    suffix.insert(0, kIndexDigits - suffix.size(), '0');
  }
  return base.string() + "." + suffix;
}

StatusOr<uint64_t> Logger::FindHighestSegment(const std::filesystem::path& base,
                                              bool* any) {
  *any = false;
  const std::filesystem::path dir =
      base.has_parent_path() ? base.parent_path() : std::filesystem::path(".");
  const std::string prefix = base.filename().string() + ".";
  std::error_code ec;
  std::filesystem::directory_iterator it(dir, ec);
  if (ec) {
    return StatusError(StatusCode::kIOError,
                       "Failed to scan " + dir.string() + ": " + ec.message());
  }
  // Segment 0 doubles as the base path (see SegmentPath), so an existing
  // unsuffixed file means the series starts at 0 even when no suffixed
  // segment exists yet.
  uint64_t highest = 0;
  if (std::filesystem::exists(base, ec) && !ec) {
    *any = true;
  }
  ec.clear();
  for (const std::filesystem::directory_entry& entry : it) {
    if (!entry.is_regular_file(ec)) {
      continue;
    }
    const std::string name = entry.path().filename().string();
    if (!name.starts_with(prefix)) {
      continue;
    }
    uint64_t index = 0;
    if (!ParseSegmentSuffix(name, &index)) {
      continue;
    }
    *any = true;
    highest = std::max(highest, index);
  }
  return highest;
}

StatusOr<uint64_t> Logger::FindLowestSegment(const std::filesystem::path& base,
                                             bool* any) {
  *any = false;
  const std::filesystem::path dir =
      base.has_parent_path() ? base.parent_path() : std::filesystem::path(".");
  const std::string prefix = base.filename().string() + ".";
  std::error_code ec;
  std::filesystem::directory_iterator it(dir, ec);
  if (ec) {
    return StatusError(StatusCode::kIOError,
                       "Failed to scan " + dir.string() + ": " + ec.message());
  }
  uint64_t lowest = std::numeric_limits<uint64_t>::max();
  if (std::filesystem::exists(base, ec) && !ec) {
    *any = true;
    lowest = 0;
  }
  ec.clear();
  for (const std::filesystem::directory_entry& entry : it) {
    if (!entry.is_regular_file(ec)) {
      continue;
    }
    const std::string name = entry.path().filename().string();
    if (!name.starts_with(prefix)) {
      continue;
    }
    uint64_t index = 0;
    if (!ParseSegmentSuffix(name, &index)) {
      continue;
    }
    *any = true;
    lowest = std::min(lowest, index);
  }
  return *any ? lowest : uint64_t{0};
}

// A database written before WAL segmentation keeps one `<name>.log` byte
// file whose offsets double as absolute LSNs. The base file itself is
// segment 0, so content within the first segment needs no work at all; only
// an oversized legacy log has its tail folded into suffixed segments 1+
// before the base is trimmed to exactly one segment.
Status Logger::MigrateLegacyLog(const std::filesystem::path& base,
                                size_t segment_size) {
  std::error_code ec;
  const auto file_size = std::filesystem::file_size(base, ec);
  if (ec) {
    return Status::kSuccess;  // No legacy file: nothing to migrate.
  }
  if (file_size == 0) {
    // An empty leftover: nothing to fold; CreateFile will reuse the name.
    return Status::kSuccess;
  }
  if (file_size <= segment_size) {
    // The base file already is segment 0; keep it byte-for-byte.
    return Status::kSuccess;
  }
  // O_RDWR: the trim step ftruncates this descriptor after the fold.
  const int src =
      platform::OpenFile(base, {.mode = platform::OpenMode::kReadWrite});
  if (src < 0) {
    return StatusError(StatusCode::kIOError,
                       "Failed to open legacy log " + base.string() + ": " +
                           std::string(std::strerror(errno)));
  }
  auto close_src = [&src] { platform::CloseFile(src); };
  std::vector<char> chunk(segment_size);
  uint64_t index = 1;
  for (uint64_t offset = segment_size; offset < file_size;
       offset += segment_size) {
    const size_t want = static_cast<size_t>(
        std::min<uint64_t>(segment_size, file_size - offset));
    const int dst = platform::OpenFile(SegmentPath(base, index),
                                       {.mode = platform::OpenMode::kReadWrite,
                                        .create = true,
                                        .truncate = true},
                                       0666);
    if (dst < 0) {
      close_src();
      return StatusError(StatusCode::kIOError,
                         "Failed to create WAL segment " +
                             SegmentPath(base, index).string() + ": " +
                             std::string(std::strerror(errno)));
    }
    size_t done = 0;
    while (done < want) {
      const ssize_t n = platform::Pread(src, chunk.data(), want - done,
                                        static_cast<int64_t>(offset + done));
      if (n <= 0) {
        platform::CloseFile(dst);
        close_src();
        return StatusError(StatusCode::kIOError,
                           "Failed to read legacy log " + base.string());
      }
      size_t written = 0;
      const auto chunk_size = static_cast<size_t>(n);
      while (written < chunk_size) {
        const ssize_t w =
            platform::Write(dst, chunk.data() + written, chunk_size - written);
        if (w < 0) {
          if (errno == EINTR) {
            continue;
          }
          platform::CloseFile(dst);
          close_src();
          return StatusError(StatusCode::kIOError,
                             "Failed to write WAL segment " +
                                 SegmentPath(base, index).string() + ": " +
                                 std::string(std::strerror(errno)));
        }
        written += static_cast<size_t>(w);
      }
      done += static_cast<size_t>(n);
    }
    if (FdataSync(dst) != 0) {
      platform::CloseFile(dst);
      close_src();
      return StatusError(
          StatusCode::kIOError,
          "Failed to fsync WAL segment " + SegmentPath(base, index).string());
    }
    platform::CloseFile(dst);
    ++index;
  }
  RETURN_IF_FAIL(SyncDirectoryOf(base));
  if (platform::Truncate(src, static_cast<int64_t>(segment_size)) != 0) {
    const int saved = errno;
    close_src();
    return StatusError(StatusCode::kIOError,
                       "Failed to trim legacy log " + base.string() + ": " +
                           std::string(std::strerror(saved)));
  }
  if (FdataSync(src) != 0) {
    const int saved = errno;
    close_src();
    return StatusError(StatusCode::kIOError,
                       "Failed to fsync trimmed legacy log " + base.string() +
                           ": " + std::string(std::strerror(saved)));
  }
  close_src();
  RETURN_IF_FAIL(SyncDirectoryOf(base));
  LOG(INFO) << "Folded legacy WAL " << base.string() << " (" << file_size
            << " bytes) into " << index << " overflow segment file(s)";
  return Status::kSuccess;
}

StatusOr<std::unique_ptr<Logger>> Logger::Create(
    const std::filesystem::path& logfile, size_t buffer_size, size_t every_ms,
    size_t segment_size, bool preallocate_new_series, bool allow_torn_footer) {
  if (segment_size == 0) {
    return StatusError(StatusCode::kIOError, "WAL segment size must be > 0");
  }
  // A crash between WriteSegmentMetadata's temp write and its rename leaves
  // the temp behind; the next write overwrites it, so removal is pure
  // hygiene a long-lived deployment should not skip.
  {
    std::error_code remove_ec;
    const std::filesystem::path stale_meta_tmp =
        SegmentMetadataPath(logfile).string() + ".tmp";
    if (std::filesystem::exists(stale_meta_tmp, remove_ec) && !remove_ec) {
      std::filesystem::remove(stale_meta_tmp, remove_ec);
      LOG(INFO) << "Removed stale WAL segment metadata temp "
                << stale_meta_tmp.string()
                << (remove_ec ? (" (failed: " + remove_ec.message() + ")")
                              : "");
    }
  }
  bool any_segment = false;
  ASSIGN_OR_RETURN(uint64_t, initial_highest,
                   FindHighestSegment(logfile, &any_segment));
  const bool has_numbered_segments = initial_highest > 0;
  bool metadata_exists = false;
  bool fixed_size = false;
  ASSIGN_OR_RETURN(size_t, persisted_segment_size,
                   ReadSegmentMetadata(logfile, &metadata_exists, &fixed_size));
  if (!metadata_exists && any_segment && !has_numbered_segments) {
    // A lost sidecar must not turn a positively identified fixed frame into
    // a legacy oversized log and migrate its footer as record bytes.
    int64_t base_size = 0;
    const int base_fd =
        platform::OpenFile(logfile, {.mode = platform::OpenMode::kReadOnly});
    bool framed = false;
    if (base_fd >= 0) {
      if (platform::FileSize(base_fd, &base_size) == 0 &&
          std::cmp_greater(base_size, kTailBytes)) {
        const auto candidate_size = static_cast<size_t>(base_size) - kTailBytes;
        framed = ReadFixedTail(base_fd, 0, candidate_size, false).HasValue();
      }
      platform::CloseFile(base_fd);
    }
    if (framed) {
      return StatusError(
          StatusCode::kCorrupt,
          "Fixed WAL has no geometry metadata: " + logfile.string());
    }
  }
  if (!metadata_exists && !any_segment && preallocate_new_series) {
    fixed_size = true;
  }
  if (metadata_exists && (has_numbered_segments || fixed_size)) {
    if (persisted_segment_size != segment_size) {
      LOG(WARN) << "Ignoring WAL segment size override " << segment_size
                << "; persisted geometry is " << persisted_segment_size;
    }
    segment_size = persisted_segment_size;
  } else if (has_numbered_segments) {
    return StatusError(
        StatusCode::kIOError,
        "Segmented WAL has no geometry metadata: " + logfile.string());
  } else if (!metadata_exists || persisted_segment_size != segment_size) {
    RETURN_IF_FAIL(WriteSegmentMetadata(logfile, segment_size, fixed_size));
  }
  bool legacy_base_oversized = false;
  {
    std::error_code size_error;
    const bool base_exists = std::filesystem::exists(logfile, size_error);
    if (size_error) {
      return StatusError(StatusCode::kIOError, "Failed to stat legacy WAL " +
                                                   logfile.string() + ": " +
                                                   size_error.message());
    }
    if (base_exists) {
      const auto base_size = std::filesystem::file_size(logfile, size_error);
      if (size_error) {
        return StatusError(StatusCode::kIOError, "Failed to stat legacy WAL " +
                                                     logfile.string() + ": " +
                                                     size_error.message());
      }
      legacy_base_oversized = base_size > segment_size;
    }
  }
  if (!fixed_size && (!has_numbered_segments || legacy_base_oversized)) {
    RETURN_IF_FAIL(MigrateLegacyLog(logfile, segment_size));
  }
  ASSIGN_OR_RETURN(uint64_t, start_segment,
                   FindHighestSegment(logfile, &any_segment));
  const auto start_path = SegmentPath(logfile, start_segment);
  std::error_code existed_ec;
  const bool existed = std::filesystem::exists(start_path, existed_ec);
  if (existed_ec) {
    return StatusError(StatusCode::kIOError,
                       "Failed to stat fixed WAL segment");
  }
  ASSIGN_OR_RETURN(int, fd, CreateFile(start_path));
  FixedTail fixed_tail;
  if (fixed_size) {
    StatusOr<FixedTail> tail =
        existed
            ? ReadFixedTail(fd, start_segment, segment_size, allow_torn_footer)
            : StatusOr<FixedTail>(FixedTail{});
    if (!tail.HasValue()) {
      platform::CloseFile(fd);
      return tail.GetStatus();
    }
    fixed_tail = tail.Value();
    // Rewriting both slots is only needed for a new or explicitly repaired
    // frame. A normal reopen keeps the alternating generation history.
    const bool needs_init =
        !existed ||
        !ReadFixedTail(fd, start_segment, segment_size, false).HasValue();
    if (ClearAppend(fd) != 0 ||
        (needs_init && InitializeFixedFile(fd, start_segment, segment_size,
                                           fixed_tail.end, !existed) != 0)) {
      const int saved = errno;
      platform::CloseFile(fd);
      return StatusError(StatusCode::kIOError,
                         "Failed to initialize fixed WAL: " +
                             std::string(std::strerror(saved)));
    }
  }
  Status directory_status =
      SyncDirectoryOf(SegmentPath(logfile, start_segment));
  if (directory_status != Status::kSuccess) {
    platform::CloseFile(fd);
    return directory_status;
  }
  // Seed the recycle scan cursor: everything below the lowest existing
  // segment is already gone (previous incarnation recycled it), so the
  // first RecycleUpTo must not walk that prefix.
  bool any_lowest = false;
  uint64_t lowest_segment = 0;
  if (auto scanned = FindLowestSegment(logfile, &any_lowest);
      scanned.HasValue() && any_lowest) {
    lowest_segment = scanned.Value();
  }
  std::error_code ec;
  // All state must be consistent before the worker starts; otherwise the
  // worker may observe flushed_lsn_ == 0 and append zeros over an existing
  // WAL. The start LSN is the absolute end of the current segment.
  const auto seg_size =
      std::filesystem::file_size(SegmentPath(logfile, start_segment), ec);
  if (ec) {
    platform::CloseFile(fd);
    return StatusError(StatusCode::kIOError,
                       "Failed to stat log segment " +
                           SegmentPath(logfile, start_segment).string() + ": " +
                           ec.message());
  }
  const lsn_t file_size =
      (static_cast<lsn_t>(start_segment) * static_cast<lsn_t>(segment_size)) +
      static_cast<lsn_t>(fixed_size ? fixed_tail.end : seg_size);
  if (FdataSync(fd) != 0) {
    const int saved = errno;
    platform::CloseFile(fd);
    return StatusError(StatusCode::kIOError,
                       "Failed to fsync existing WAL segment before reopen: " +
                           std::string(std::strerror(saved)));
  }
  // std::thread has no noexcept construction path; EAGAIN/ENOMEM from the
  // pthread layer is caught at this boundary and mapped to a Status.
  std::unique_ptr<Logger> logger;
  try {
    logger = std::unique_ptr<Logger>(
        new Logger(logfile, buffer_size, every_ms, segment_size));  // NOLINT
  } catch (const std::bad_alloc&) {
    // The fd is still owned by this frame; ~Logger has not run, so close it
    // here or the descriptor leaks on an allocation failure.
    platform::CloseFile(fd);
    return StatusError(StatusCode::kIOError,
                       "Failed to allocate logger for " + logfile.string());
  }
  // Transfer fd ownership before anything that can throw: the destructor
  // closes dst_, so a Logger constructor failure must not leave the fd
  // stored nowhere, and a spawn failure must not double-close.
  logger->dst_ = fd;
  logger->current_segment_ = start_segment;
  logger->fixed_size_segments_ = fixed_size;
  logger->footer_generation_ = fixed_tail.generation;
  logger->lowest_segment_ = lowest_segment;
  logger->flushed_lsn_.store(file_size, std::memory_order_relaxed);
  logger->durable_lsn_.store(file_size, std::memory_order_release);
  logger->buffered_lsn_.store(file_size, std::memory_order_release);
  // The ring starts zeroed: every byte below the open-time WAL end lives
  // only in the segment files, and TryCopyBuffered must never serve the
  // zeroed ring contents for them.
  logger->ring_floor_lsn_.store(file_size, std::memory_order_release);
  try {
    logger->worker_ = std::thread(&Logger::LoggerWork, logger.get());
  } catch (const std::system_error& error) {
    // ~Logger already closed dst_ via the unique_ptr destruction above.
    return StatusError(StatusCode::kIOError, "Failed to start logger worker: " +
                                                 std::string(error.what()));
  } catch (...) {
    return StatusError(StatusCode::kIOError, "Failed to start logger worker");
  }
  return logger;
}

Logger::Logger(std::filesystem::path logfile, size_t buffer_size,
               size_t every_ms, size_t segment_size)
    : base_path_(std::move(logfile)),
      segment_size_(segment_size),
      buffer_(std::max<size_t>(1, buffer_size), 0),
      sync_interval_(std::chrono::milliseconds(std::max<size_t>(1, every_ms))) {
}

Logger::~Logger() {
  DrainAndStopWorker();
  if (dst_ >= 0) {
    platform::CloseFile(dst_);
    dst_ = -1;
  }
}

void Logger::NotifyWorker() {
  // Notify under work_mu_: the full-buffer waiters in AddLog evaluate their
  // predicate and block under this mutex, so an unlocked notify can fire
  // between the predicate check and the block and be lost forever (the
  // idle worker never re-notifies work_cv_ on its own).  No caller holds
  // work_mu_ here, and the state stores that precede every call become
  // visible to a later waiter through the mutex.
  std::scoped_lock lock(work_mu_);
  work_cv_.notify_all();
}

void Logger::SetFailed(int err) {
  error_number_.store(err, std::memory_order_relaxed);
  failed_.store(true, std::memory_order_release);
  // Name the volume state for the two failure classes an operator can act
  // on without touching the engine: ENOSPC (free space, or the inode/tmpfs
  // quota that shows up as ENOSPC on shared volumes) and EIO (device
  // trouble). Every transaction fails after this line, so this one line is
  // the postmortem anchor.
#if defined(EDQUOT)
  const bool is_space_err = (err == ENOSPC || err == EDQUOT);
#else
  const bool is_space_err = (err == ENOSPC);
#endif
  if (is_space_err) {
    std::error_code ec;
    const auto free_bytes = std::filesystem::space(
        std::filesystem::path(base_path_).parent_path(), ec);
    if (!ec) {
      LOG(ERROR) << std::strerror(err) << " on WAL volume of " << base_path_
                 << ": " << (free_bytes.available >> 20) << " MiB free of "
                 << (free_bytes.capacity >> 20) << " MiB";
    } else {
      LOG(ERROR) << std::strerror(err) << " on WAL " << base_path_;
    }
  } else {
    LOG(ERROR) << std::strerror(err) << " (WAL " << base_path_ << ")";
  }
  {
    std::scoped_lock lock(durable_mu_);
  }
  durable_cv_.notify_all();
  {
    std::scoped_lock lock(work_mu_);
  }
  work_cv_.notify_all();
}

Status Logger::CheckFailed() const {
  if (!failed_.load(std::memory_order_acquire)) {
    return Status::kSuccess;
  }
  return StatusError(StatusCode::kIOError,
                     "Logger write failed: " +
                         std::string(std::strerror(
                             error_number_.load(std::memory_order_acquire))));
}

void Logger::AdvanceDurable(lsn_t to) {
  {
    std::scoped_lock lock(durable_mu_);
    if (to > durable_lsn_.load(std::memory_order_relaxed)) {
      durable_lsn_.store(to, std::memory_order_release);
    }
  }
  durable_cv_.notify_all();
}

void Logger::SetBeforeSyncHookForTest(std::function<int(int)> hook) {
  std::scoped_lock lock(sync_hook_mutex);
  before_sync_hook = std::move(hook);
}

int Logger::WriteFixedTail(size_t logical_end) {
  if (logical_end > segment_size_ ||
      footer_generation_ == std::numeric_limits<uint64_t>::max()) {
    errno = EOVERFLOW;
    return -1;
  }
  const uint64_t generation = footer_generation_ + 1;
  const auto bytes = EncodeTail(current_segment_, generation, logical_end);
  if (PwriteAll(dst_, bytes.data(), bytes.size(),
                static_cast<int64_t>(segment_size_ + ((generation % 2) *
                                                      kTailSlotBytes))) != 0) {
    return -1;
  }
  footer_generation_ = generation;
  return 0;
}

int Logger::SyncSegment(size_t logical_end) {
  if (fixed_size_segments_) {
    if (logical_end == std::numeric_limits<size_t>::max()) {
      const lsn_t start = current_segment_ * static_cast<lsn_t>(segment_size_);
      const lsn_t written = flushed_lsn_.load(std::memory_order_relaxed);
      logical_end = static_cast<size_t>(std::min<lsn_t>(
          written >= start ? written - start : 0, segment_size_));
    }
    if (WriteFixedTail(logical_end) != 0) {
      return -1;
    }
  }
  std::function<int(int)> hook;
  {
    std::scoped_lock lock(sync_hook_mutex);
    hook = before_sync_hook;
  }
  if (hook) {
    const int error = hook(dst_);
    if (error != 0) {
      errno = error;
      return -1;
    }
  }
  const auto begin = std::chrono::steady_clock::now();
  const int result = FdataSync(dst_);
  const int saved = errno;
  sync_count_.fetch_add(1, std::memory_order_relaxed);
  sync_ns_.fetch_add(static_cast<uint64_t>(
                         std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now() - begin)
                             .count()),
                     std::memory_order_relaxed);
  errno = saved;
  return result;
}

Status Logger::WaitForDurable(lsn_t lsn) {
  pending_durable_waiters_.fetch_add(1, std::memory_order_acq_rel);
  const bool already_satisfied =
      failed_.load(std::memory_order_acquire) ||
      finish_.load(std::memory_order_acquire) ||
      durable_lsn_.load(std::memory_order_acquire) >= lsn;
  if (!already_satisfied) {
    // Nudge the worker: it will fsync promptly when it sees the non-zero
    // waiter count even if the timer has not elapsed.
    NotifyWorker();
    std::unique_lock lock(durable_mu_);
    durable_cv_.wait(lock, [&] {
      return failed_.load(std::memory_order_acquire) ||
             finish_.load(std::memory_order_acquire) ||
             durable_lsn_.load(std::memory_order_acquire) >= lsn;
    });
  }
  pending_durable_waiters_.fetch_sub(1, std::memory_order_acq_rel);
  Status failed = CheckFailed();
  if (failed != Status::kSuccess) {
    return failed;
  }
  if (finish_.load(std::memory_order_acquire) &&
      durable_lsn_.load(std::memory_order_acquire) < lsn) {
    return StatusError(StatusCode::kRuntimeError,
                       "logger finished before requested LSN became durable");
  }
  return Status::kSuccess;
}

Status Logger::RollToLocked(uint64_t index) {
  if (dst_ >= 0) {
    int64_t file_size = 0;
    if (platform::FileSize(dst_, &file_size) != 0) {
      const int saved = errno == 0 ? EIO : errno;
      platform::CloseFile(dst_);
      dst_ = -1;
      return StatusError(StatusCode::kIOError,
                         "Failed to stat WAL segment before roll: " +
                             std::string(std::strerror(saved)));
    }
    if (file_size < 0) {
      platform::CloseFile(dst_);
      dst_ = -1;
      return StatusError(StatusCode::kIOError,
                         "WAL segment size is negative before roll");
    }
    if (fixed_size_segments_ &&
        std::cmp_not_equal(file_size, segment_size_ + kTailBytes)) {
      platform::CloseFile(dst_);
      dst_ = -1;
      return StatusError(StatusCode::kCorrupt,
                         "Fixed WAL frame length changed before roll");
    }
    if (!fixed_size_segments_ && std::cmp_less(file_size, segment_size_)) {
      if (platform::Truncate(dst_, static_cast<int64_t>(segment_size_)) != 0) {
        const int saved = errno;
        platform::CloseFile(dst_);
        dst_ = -1;
        return StatusError(StatusCode::kIOError,
                           "Failed to extend WAL segment before roll: " +
                               std::string(std::strerror(saved)));
      }
    } else if (std::cmp_greater(
                   file_size,
                   segment_size_ + (fixed_size_segments_ ? kTailBytes : 0))) {
      platform::CloseFile(dst_);
      dst_ = -1;
      return StatusError(StatusCode::kCorrupt,
                         "WAL segment is larger than its configured geometry");
    }
    // The old segment will never be fsynced through the new fd: flush it
    // here, or records already reported durable via the new segment's
    // barriers could sit unsynced in the closed file forever.
    if ((!fixed_size_segments_ ||
         durable_lsn_.load(std::memory_order_acquire) <
             flushed_lsn_.load(std::memory_order_relaxed)) &&
        SyncSegment() != 0) {
      const int saved = errno;
      platform::CloseFile(dst_);
      dst_ = -1;
      return StatusError(StatusCode::kIOError,
                         "Failed to fsync WAL segment before roll: " +
                             std::string(std::strerror(saved)));
    }
    platform::CloseFile(dst_);
    dst_ = -1;
  }
  if (fixed_size_segments_) {
    std::error_code ec;
    const bool present =
        std::filesystem::exists(SegmentPath(base_path_, index), ec);
    if (ec || present) {
      return StatusError(
          StatusCode::kCorrupt,
          "Refusing to overwrite an existing future WAL segment");
    }
  }
  ASSIGN_OR_RETURN(int, fd, CreateFile(SegmentPath(base_path_, index)));
  if (fixed_size_segments_ &&
      InitializeFixedFile(fd, index, segment_size_, 0, true) != 0) {
    const int saved = errno;
    platform::CloseFile(fd);
    return StatusError(StatusCode::kIOError,
                       "Failed to allocate next WAL segment: " +
                           std::string(std::strerror(saved)));
  }
  footer_generation_ = 0;
  Status directory_status = SyncDirectoryOf(SegmentPath(base_path_, index));
  if (directory_status != Status::kSuccess) {
    platform::CloseFile(fd);
    return directory_status;
  }
  dst_ = fd;
  current_segment_ = index;
  return Status::kSuccess;
}

Status Logger::TruncateTo(lsn_t valid_end) {
  if (finish_.load(std::memory_order_acquire)) {
    return StatusError(StatusCode::kRuntimeError, "logger is finished");
  }
  std::unique_lock enq_lk{enqueue_latch_};
  if (finish_.load(std::memory_order_acquire)) {
    return StatusError(StatusCode::kRuntimeError, "logger is finished");
  }
  for (;;) {
    RETURN_IF_FAIL(CheckFailed());
    const lsn_t flushed = flushed_lsn_.load(std::memory_order_acquire);
    const lsn_t buffered = buffered_lsn_.load(std::memory_order_acquire);
    if (flushed >= buffered) {
      break;
    }
    NotifyWorker();
    std::unique_lock work_lk(work_mu_);
    work_cv_.wait(work_lk, [&] {
      return failed_.load(std::memory_order_acquire) ||
             flushed_lsn_.load(std::memory_order_acquire) >=
                 buffered_lsn_.load(std::memory_order_acquire);
    });
  }
  if (finish_.load(std::memory_order_acquire)) {
    return StatusError(StatusCode::kRuntimeError, "logger is finished");
  }

  std::scoped_lock segment_guard(segment_mu_);
  const lsn_t flushed = flushed_lsn_.load(std::memory_order_acquire);
  if (valid_end > flushed) {
    return StatusError(StatusCode::kInvalidArgument,
                       "Cannot truncate WAL past its written end");
  }
  const lsn_t retained_start =
      static_cast<lsn_t>(lowest_segment_) * static_cast<lsn_t>(segment_size_);
  if (valid_end < retained_start) {
    return StatusError(StatusCode::kCorrupt,
                       "WAL truncation is below the retained segment start");
  }
  if (dst_ >= 0) {
    platform::CloseFile(dst_);
    dst_ = -1;
  }

  const uint64_t tail_index = SegmentIndex(valid_end, segment_size_);
  bool any_segment = false;
  ASSIGN_OR_RETURN(uint64_t, highest,
                   FindHighestSegment(base_path_, &any_segment));
  if (!any_segment) {
    highest = 0;
  }
  for (uint64_t index = tail_index + 1; index <= highest; ++index) {
    const std::filesystem::path path = SegmentPath(base_path_, index);
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
      SetFailed(EIO);
      return StatusError(StatusCode::kIOError,
                         "Failed to remove stale WAL segment " + path.string() +
                             ": " + ec.message());
    }
  }

  const std::filesystem::path tail = SegmentPath(base_path_, tail_index);
  StatusOr<int> opened_tail = CreateFile(tail);
  if (!opened_tail.HasValue()) {
    SetFailed(EIO);
    return opened_tail.GetStatus();
  }
  const int tail_fd = opened_tail.Value();
  if ((fixed_size_segments_
           ? InitializeFixedFile(tail_fd, tail_index, segment_size_,
                                 static_cast<size_t>(valid_end % segment_size_))
           : platform::Truncate(
                 tail_fd, static_cast<int64_t>(valid_end % segment_size_))) !=
      0) {
    const int saved = errno;
    platform::CloseFile(tail_fd);
    SetFailed(saved);
    return StatusError(StatusCode::kIOError, "Failed to truncate WAL segment " +
                                                 tail.string() + ": " +
                                                 std::strerror(saved));
  }
  if (FdataSync(tail_fd) != 0) {
    const int saved = errno;
    platform::CloseFile(tail_fd);
    SetFailed(saved);
    return StatusError(StatusCode::kIOError,
                       std::string("Failed to fsync truncated WAL segment: ") +
                           std::string(std::strerror(saved)));
  }
  Status directory_status = SyncDirectoryOf(base_path_);
  if (directory_status != Status::kSuccess) {
    platform::CloseFile(tail_fd);
    SetFailed(EIO);
    return directory_status;
  }

  dst_ = tail_fd;
  current_segment_ = tail_index;
  footer_generation_ = 0;
  flushed_lsn_.store(valid_end, std::memory_order_relaxed);
  durable_lsn_.store(valid_end, std::memory_order_release);
  buffered_lsn_.store(valid_end, std::memory_order_release);
  // Everything below the new end predates the truncation: the ring holds no
  // valid bytes for it (post-truncation appends start at valid_end).
  ring_floor_lsn_.store(valid_end, std::memory_order_release);
  durable_cv_.notify_all();
  NotifyWorker();
  return Status::kSuccess;
}

bool Logger::TryCopyBuffered(lsn_t lsn, char* dst, size_t len) const {
  if (len == 0) {
    return true;
  }
  const size_t cap = buffer_.size();
  if (cap == 0) {
    return false;
  }
  const lsn_t buffered = buffered_lsn_.load(std::memory_order_acquire);
  // Oldest byte the ring still holds: positions below (buffered - cap) were
  // overwritten by the wrap. Bytes below ring_floor_lsn_ never passed through
  // this ring at all (a reopened database's ring starts zeroed).
  const lsn_t oldest_ring = buffered > static_cast<lsn_t>(cap)
                                ? buffered - static_cast<lsn_t>(cap)
                                : 0;
  const lsn_t window_start =
      std::max(ring_floor_lsn_.load(std::memory_order_acquire), oldest_ring);
  const lsn_t end = lsn + static_cast<lsn_t>(len);
  if (lsn < window_start || end > buffered) {
    return false;
  }
  auto pos = static_cast<size_t>(lsn % static_cast<lsn_t>(cap));
  size_t done = 0;
  while (done < len) {
    const size_t chunk = std::min(len - done, cap - pos);
    std::memcpy(dst + done, buffer_.data() + pos, chunk);
    done += chunk;
    pos += chunk;
    if (pos == cap) {
      pos = 0;
    }
  }
  // Re-verify with a fresh load: producers overwrite position x only after
  // the (monotonic) buffered pointer passes x + cap, so a passing check
  // proves no byte of the copied range was rewritten while the memcpy ran.
  const lsn_t after = buffered_lsn_.load(std::memory_order_acquire);
  return after >= end && after < lsn + static_cast<lsn_t>(cap);
}

void Logger::AdviseOldBytesDurable(lsn_t before) const {
  if (before == 0) {
    return;
  }
  if constexpr (!platform::kSupportsAdviseDontNeed) {
    return;
  }
  // Drop [0, before) from page cache across the segment files: those bytes
  // are now safe to discard because the checkpoint that anchored them has
  // been durably committed to the master record. Without this hint fdatasync
  // keeps having to wait for every dirty page from the start of the WAL to
  // reach disk, which on long TPC-C runs turns a sub-millisecond barrier
  // into a multi-millisecond one.
  // Start at the lowest RETAINED segment: after a recycle the removed head
  // segments no longer exist, and breaking on the first missing file would
  // skip every existing one -- the advise silently turned into a permanent
  // no-op after the first successful recycle.
  const lsn_t lowest_retained = LowestRetainedLSN();
  uint64_t highest_segment = 0;
  {
    std::scoped_lock segment_guard(segment_mu_);
    highest_segment = current_segment_;
  }
  auto start_index = static_cast<uint64_t>(lowest_retained /
                                           static_cast<lsn_t>(segment_size_));
  if (start_index > highest_segment) {
    return;
  }
  std::error_code ec;
  for (uint64_t index = start_index;; ++index) {
    if (index > highest_segment) {
      break;
    }
    const std::filesystem::path path = SegmentPath(base_path_, index);
    if (!std::filesystem::exists(path, ec) || ec) {
      break;
    }
    const lsn_t segment_start =
        static_cast<lsn_t>(index) * static_cast<lsn_t>(segment_size_);
    const int fd =
        platform::OpenFile(path, {.mode = platform::OpenMode::kReadOnly});
    if (fd < 0) {
      break;
    }
    const lsn_t advise_from = std::max(segment_start, lsn_t{0});
    const lsn_t advise_to =
        std::min<lsn_t>(segment_start + segment_size_, before);
    if (advise_to > advise_from) {
      platform::AdviseDontNeed(
          fd, static_cast<int64_t>(advise_from - segment_start),
          static_cast<int64_t>(advise_to - advise_from));
    }
    platform::CloseFile(fd);
  }
}

void Logger::SetRecycleBoundaryValidator(std::function<bool(lsn_t)> validator) {
  recycle_boundary_validator_ = std::move(validator);
}

lsn_t Logger::RecycleUpTo(lsn_t floor) {
  std::scoped_lock segment_guard(segment_mu_);
  lsn_t recycled_to = 0;
  // Snapshot the live segment index once: the flush worker may roll to a
  // fresh segment concurrently, and that segment is never a recycle
  // candidate (its records sit above any floor the checkpoint computed).
  const uint64_t highest_deletable = current_segment_;
  std::error_code ec;
  // Start at the retained start, not at 0: every index below lowest_segment_
  // was removed by an earlier recycle (or never existed), and re-walking
  // that prefix would make checkpoint cost grow with the total WAL history
  // ever written while holding segment_mu_ -- the mutex the WAL writer takes
  // for every segment lookup.
  for (uint64_t index = lowest_segment_; index < highest_deletable; ++index) {
    // Whole-segment rule: a segment is removable only when every byte it can
    // hold sits below the floor, i.e. its end ((index+1) * segment_size) is
    // at or below `floor`.
    if (static_cast<lsn_t>(index + 1) * static_cast<lsn_t>(segment_size_) >
        floor) {
      break;
    }
    // Record-boundary rule: the next segment's first byte becomes the
    // retained start, so it must begin a record. A record straddling the
    // (index, index+1) boundary keeps its head in segment (index); while it
    // lives, segment (index) cannot be removed.
    if (recycle_boundary_validator_ &&
        !recycle_boundary_validator_(static_cast<lsn_t>(index + 1) *
                                     static_cast<lsn_t>(segment_size_))) {
      break;
    }
    const std::filesystem::path path = SegmentPath(base_path_, index);
    if (!std::filesystem::exists(path, ec) || ec) {
      // Already gone (earlier recycle or external cleanup): keep the cursor
      // honest so this index is never re-scanned.
      lowest_segment_ = index + 1;
      continue;
    }
    if (!std::filesystem::remove(path, ec) || ec) {
      LOG(WARN) << "Failed to recycle WAL segment " << path.string() << ": "
                << ec.message();
      break;  // Keep the tail contiguous: never create a hole.
    }
    recycled_to =
        static_cast<lsn_t>(index + 1) * static_cast<lsn_t>(segment_size_);
    lowest_segment_ = index + 1;
  }
  if (recycled_to != 0) {
    const Status directory_status = SyncDirectoryOf(base_path_);
    if (directory_status != Status::kSuccess) {
      LOG(ERROR) << "WAL directory sync failed after recycling: "
                 << directory_status;
    }
    LOG(INFO) << "Recycled WAL segments below LSN " << recycled_to;
  }
  return recycled_to;
}

lsn_t Logger::LowestRetainedLSN() const {
  uint64_t highest_segment = 0;
  uint64_t start = 0;
  {
    std::scoped_lock segment_guard(segment_mu_);
    highest_segment = current_segment_;
    // Start from the tracked retained start; the existence probe only walks
    // a gap when segments were removed behind our back (operator cleanup).
    start = lowest_segment_;
  }
  std::error_code ec;
  for (uint64_t index = start; index <= highest_segment; ++index) {
    const std::filesystem::path path = SegmentPath(base_path_, index);
    if (std::filesystem::exists(path, ec) && !ec) {
      return static_cast<lsn_t>(index) * static_cast<lsn_t>(segment_size_);
    }
    ec.clear();
  }
  // No segment below the current one exists: the current segment is the
  // retained start.
  return static_cast<lsn_t>(highest_segment) *
         static_cast<lsn_t>(segment_size_);
}

void Logger::DrainAndStopWorker() {
  std::unique_lock enq_lk{enqueue_latch_};
  finish_.store(true, std::memory_order_release);
  durable_cv_.notify_all();
  NotifyWorker();
  while (!failed_.load(std::memory_order_acquire) &&
         flushed_lsn_.load(std::memory_order_acquire) <
             buffered_lsn_.load(std::memory_order_acquire)) {
    std::unique_lock lock(work_mu_);
    work_cv_.wait_for(lock, std::chrono::milliseconds(10), [&] {
      return failed_.load(std::memory_order_acquire) ||
             flushed_lsn_.load(std::memory_order_acquire) >=
                 buffered_lsn_.load(std::memory_order_acquire);
    });
  }
  if (worker_.joinable()) {
    worker_.join();
  }
}

Status Logger::Finish() {
  DrainAndStopWorker();
  return CheckFailed();
}

StatusOr<lsn_t> Logger::AddLog(std::string_view payload) {
  RETURN_IF_FAIL(CheckFailed());
  if (finish_.load(std::memory_order_acquire)) {
    return StatusError(StatusCode::kRuntimeError, "logger is finished");
  }
  // D1: refuse records the recovery reader could never parse back.
  if (payload.size() > kMaxRecordSize) {
    return StatusError(StatusCode::kTooBigData,
                       "WAL record exceeds Logger::kMaxRecordSize: " +
                           std::to_string(payload.size()) + " > " +
                           std::to_string(kMaxRecordSize));
  }
  if (payload.size() > segment_size_) {
    return StatusError(StatusCode::kTooBigData,
                       "WAL record exceeds the configured segment size: " +
                           std::to_string(payload.size()) + " > " +
                           std::to_string(segment_size_));
  }
  std::unique_lock enq_lk{enqueue_latch_};
  if (finish_.load(std::memory_order_acquire)) {
    return StatusError(StatusCode::kRuntimeError, "logger is finished");
  }
  lsn_t lsn = buffered_lsn_.load(std::memory_order_relaxed);
  // Pre-roll keeps the next segment on a record boundary for recycling.
  // A framed old segment keeps its actual data end in the CRC slots; recovery
  // skips only the positively declared unused suffix, never arbitrary zeros.
  if (!payload.empty() &&
      payload.size() >
          segment_size_ - static_cast<size_t>(lsn % segment_size_)) {
    RETURN_IF_FAIL(WaitForDurable(lsn));
    if (finish_.load(std::memory_order_acquire)) {
      return StatusError(StatusCode::kRuntimeError, "logger is finished");
    }
    std::scoped_lock segment_guard(segment_mu_);
    RETURN_IF_FAIL(CheckFailed());
    if (flushed_lsn_.load(std::memory_order_acquire) < lsn ||
        durable_lsn_.load(std::memory_order_acquire) < lsn) {
      SetFailed(EIO);
      return StatusError(StatusCode::kIOError,
                         "WAL segment roll lost its durability barrier");
    }
    const uint64_t next_segment = SegmentIndex(lsn, segment_size_) + 1;
    if (current_segment_ < next_segment) {
      const Status roll_status = RollToLocked(next_segment);
      if (roll_status != Status::kSuccess) {
        const int saved = errno;
        SetFailed(saved == 0 ? EIO : saved);
        return roll_status;
      }
    } else if (current_segment_ > next_segment) {
      SetFailed(EIO);
      return StatusError(StatusCode::kCorrupt,
                         "WAL segment cursor moved backwards");
    }
    const lsn_t next_lsn =
        static_cast<lsn_t>(next_segment) * static_cast<lsn_t>(segment_size_);
    flushed_lsn_.store(next_lsn, std::memory_order_relaxed);
    buffered_lsn_.store(next_lsn, std::memory_order_release);
    lsn = next_lsn;
  }
  while (!payload.empty()) {
    // Self-written under the enqueue latch (relaxed is enough) and read from
    // the worker's release stores (acquire orders the ring bytes we may not
    // overwrite). The former seq_cst pair cost two full fences per chunk.
    const size_t buffered_lsn = buffered_lsn_.load(std::memory_order_relaxed);
    const size_t flushed_lsn = flushed_lsn_.load(std::memory_order_acquire);

    if (buffered_lsn - flushed_lsn == buffer_.size()) {
      // D1: the enqueue latch is held until the ENTIRE record has landed in
      // the ring buffer, so no other producer can fragment this record's byte
      // stream.  Waiting for flush progress while holding the latch is safe
      // because the flush worker never takes enqueue_latch_ (see
      // docs/lock_order.md); a full buffer drains without our help.
      std::unique_lock work_lk(work_mu_);
      // Count the park so the flush worker knows a wakeup is owed: ring
      // occupancy cannot be sampled for this (a producer that observed a
      // full ring and parked may see the worker drain it from a snapshot
      // that never looked full to the worker -- the lost wakeup that
      // deadlocked LoggerTest.AppendMany's 32-byte ring).
      ++full_buffer_waiters_;
      // The park itself must wake the worker: the idle sleep's predicate
      // (flushed < buffered) is false when the ring is FULL -- every byte is
      // written, none are drained -- so a producer parking behind an already
      // idle worker used to sleep the whole sync interval before the
      // worker's timed wait re-checked. Notify under work_mu_, which the
      // park and the idle sleep share, and let the idle predicate (see
      // LoggerWork) treat a non-zero park count as work to do.
      work_cv_.notify_all();
      work_cv_.wait(work_lk, [&] {
        return failed_.load(std::memory_order_acquire) ||
               buffered_lsn_.load(std::memory_order_acquire) -
                       flushed_lsn_.load(std::memory_order_acquire) <
                   buffer_.size();
      });
      --full_buffer_waiters_;
      RETURN_IF_FAIL(CheckFailed());
      continue;
    }
    const size_t buffered = buffered_lsn % buffer_.size();
    const size_t flushed = flushed_lsn % buffer_.size();
    const size_t write_size =
        flushed <= buffered
            ? std::min(payload.size(), buffer_.size() - buffered)
            : std::min(payload.size(), flushed - buffered);
    memcpy(buffer_.data() + buffered, payload.data(), write_size);

    // Forward buffer pointer.
    buffered_lsn_.store(buffered_lsn + write_size, std::memory_order_release);
    payload.remove_prefix(write_size);
  }
  enq_lk.unlock();
  // Wake the flush worker only when it is (or was just about to be) asleep:
  // a producer that observes the worker still draining can skip the wakeup,
  // because the worker re-reads buffered_lsn_ before it can block again and
  // drains the new bytes on that pass. This drops a mutex acquisition plus a
  // futex wake from every appended record -- the dominant syscall on the
  // group-commit path. See worker_sleeping_ for the missed-wakeup bound.
  if (worker_sleeping_.load(std::memory_order_acquire)) {
    NotifyWorker();
  }
  return lsn;
}

void Logger::LoggerWork() {
  assert(!buffer_.empty());
  using Clock = std::chrono::steady_clock;
  auto last_sync = Clock::now();
  bool dirty = false;
  const auto background_interval = [&] {
    return commit_driven_sync_.load(std::memory_order_relaxed)
               ? std::max(sync_interval_, std::chrono::milliseconds(200))
               : sync_interval_;
  };
  // Adaptive idle cadence for the empty-queue sleep below; see kIdleWaitCap.
  std::chrono::milliseconds idle_wait = sync_interval_;
  while (!failed_.load(std::memory_order_acquire) &&
         (!finish_.load(std::memory_order_acquire) ||
          flushed_lsn_.load(std::memory_order_acquire) <
              buffered_lsn_.load(std::memory_order_acquire) ||
          dirty)) {
    const size_t flushed_lsn = flushed_lsn_.load(std::memory_order_relaxed);
    const size_t buffered_lsn = buffered_lsn_.load(std::memory_order_acquire);

    if (flushed_lsn == buffered_lsn) {
      if (dirty &&
          (finish_.load(std::memory_order_acquire) ||
           Clock::now() - last_sync >= background_interval() ||
           pending_durable_waiters_.load(std::memory_order_acquire) > 0)) {
        {
          std::scoped_lock segment_guard(segment_mu_);
          if (dst_ < 0 || SyncSegment() != 0) {
            const int saved = errno == 0 ? EIO : errno;
            SetFailed(saved);
            return;
          }
          last_sync = Clock::now();
        }
        dirty = false;
        AdvanceDurable(flushed_lsn_.load(std::memory_order_relaxed));
      }
      if (finish_.load(std::memory_order_acquire) &&
          flushed_lsn_.load(std::memory_order_acquire) >=
              buffered_lsn_.load(std::memory_order_acquire) &&
          !dirty) {
        break;
      }
      {
        std::unique_lock work_lk(work_mu_);
        // Advertise the sleep under the mutex producers' NotifyWorker also
        // takes: an AddLog that reads worker_sleeping_ == true is guaranteed
        // to wake us (its notify serializes with this critical section), and
        // one that reads false has already published its bytes for the next
        // predicate evaluation below. A producer parking on a FULL ring is
        // the one state occupancy predicates cannot see (every byte written,
        // none drained): its park count is part of this predicate and its
        // park notifies under this same mutex.
        worker_sleeping_.store(true, std::memory_order_release);
        work_cv_.wait_for(work_lk, idle_wait, [&] {
          return finish_.load(std::memory_order_acquire) ||
                 flushed_lsn_.load(std::memory_order_acquire) <
                     buffered_lsn_.load(std::memory_order_acquire) ||
                 full_buffer_waiters_ != 0 ||
                 pending_durable_waiters_.load(std::memory_order_acquire) > 0;
        });
        worker_sleeping_.store(false, std::memory_order_release);
      }
      // Woken with work to do (or a waiter to serve): return to the tight
      // group-commit cadence. Woken by the idle timeout with nothing to do:
      // back off toward kIdleWaitCap so a quiet database stops burning the
      // core on no-op re-arms.
      if (flushed_lsn_.load(std::memory_order_acquire) ==
              buffered_lsn_.load(std::memory_order_acquire) &&
          pending_durable_waiters_.load(std::memory_order_acquire) == 0 &&
          !finish_.load(std::memory_order_acquire)) {
        idle_wait = std::min(idle_wait * 2, kIdleWaitCap);
      } else {
        idle_wait = sync_interval_;
      }
      continue;
    }

    while (flushed_lsn_.load(std::memory_order_relaxed) <
           buffered_lsn_.load(std::memory_order_acquire)) {
      {
        std::scoped_lock segment_guard(segment_mu_);
        const size_t flushed = flushed_lsn_.load(std::memory_order_relaxed);
        const size_t buffered = buffered_lsn_.load(std::memory_order_acquire);
        if (flushed >= buffered) {
          break;
        }
        const size_t flushed_off = flushed % buffer_.size();
        const size_t buffered_off = buffered % buffer_.size();
        const size_t to_segment_end =
            segment_size_ - static_cast<size_t>(flushed % segment_size_);
        const size_t span = std::min<std::size_t>(
            (flushed_off < buffered_off ? buffered_off : buffer_.size()) -
                flushed_off,
            to_segment_end);
        const uint64_t write_segment = SegmentIndex(flushed, segment_size_);
        if (write_segment < current_segment_) {
          SetFailed(EIO);
          return;
        }
        if (write_segment != current_segment_) {
          const Status roll_status = RollToLocked(write_segment);
          if (roll_status != Status::kSuccess) {
            const int saved = errno;
            SetFailed(saved == 0 ? EIO : saved);
            return;
          }
        }
        ssize_t flushed_size = 0;
        do {
          flushed_size =
              fixed_size_segments_
                  ? platform::Pwrite(
                        dst_, buffer_.data() + flushed_off, span,
                        static_cast<int64_t>(flushed % segment_size_))
                  : platform::Write(dst_, buffer_.data() + flushed_off, span);
        } while (flushed_size < 0 && errno == EINTR);
        if (flushed_size < 0) {
          const int saved = errno;
          SetFailed(saved);
          return;
        }
        if (flushed_size == 0) {
          SetFailed(EIO);
          return;
        }
        flushed_lsn_.store(flushed + static_cast<size_t>(flushed_size),
                           std::memory_order_release);
      }
      dirty = true;
    }
    // Wake parked full-buffer producers once per pass, only when someone is
    // actually parked: the unconditional per-batch notify cost a futex wake
    // per write batch (5% of a TPC-C profile) for wakeups nobody waited for.
    // A producer that parks after this check has just published bytes, so
    // flushed < buffered keeps the main loop out of the idle sleep below
    // until the next pass runs this check again.
    {
      std::scoped_lock lock(work_mu_);
      if (full_buffer_waiters_ != 0) {
        work_cv_.notify_all();
      }
    }
    // Adaptive group commit: if any producer is blocked in WaitForDurable,
    // fsync immediately so they wake up on this iteration rather than after
    // the full sync_interval_ of latency. A bare timer would otherwise cap
    // throughput at floor(1 second / sync_interval_) * num_producers_per_fsync
    // barriers per second even when fsync itself is sub-millisecond.
    const bool timer_elapsed =
        Clock::now() - last_sync >= background_interval();
    const bool waiter_present =
        pending_durable_waiters_.load(std::memory_order_acquire) > 0;
    if (finish_.load(std::memory_order_acquire) || timer_elapsed ||
        waiter_present) {
      {
        std::scoped_lock segment_guard(segment_mu_);
        if (dst_ < 0 || SyncSegment() != 0) {
          const int saved = errno == 0 ? EIO : errno;
          SetFailed(saved);
          return;
        }
        last_sync = Clock::now();
      }
      dirty = false;
      AdvanceDurable(flushed_lsn_.load(std::memory_order_relaxed));
    }
  }
  {
    std::scoped_lock segment_guard(segment_mu_);
    if (dst_ < 0 ||
        (fixed_size_segments_ &&
         WriteFixedTail(static_cast<size_t>(
             flushed_lsn_.load() -
             (current_segment_ * static_cast<lsn_t>(segment_size_)))) != 0) ||
        FdataSync(dst_) != 0) {
      const int saved = errno == 0 ? EIO : errno;
      SetFailed(saved);
      return;
    }
  }
  AdvanceDurable(flushed_lsn_.load(std::memory_order_relaxed));
  NotifyWorker();
}

}  // namespace tinylamb
