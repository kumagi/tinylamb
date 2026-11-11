/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_COMMON_PLATFORM_FILE_IO_HPP
#define TINYLAMB_COMMON_PLATFORM_FILE_IO_HPP

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>

namespace tinylamb::platform {

enum class OpenMode : uint8_t {
  kReadOnly = 0,
  kReadWrite = 1,
};

struct OpenFlags {
  OpenMode mode = OpenMode::kReadOnly;
  bool create = false;
  bool append = false;
  bool truncate = false;
};

// Opens `path` in binary mode with close-on-exec (non-inheritable handle on
// Windows) and full POSIX-like sharing (read/write/delete on Windows).
// Returns a file descriptor >= 0 on success, or -1 with errno set on failure.
int OpenFile(const std::filesystem::path& path, OpenFlags flags,
             int mode = 0666);

// Closes a descriptor returned by OpenFile / OpenWriteFailingDescriptor.
int CloseFile(int fd);

// Duplicates `oldfd` onto `newfd` (dup2 semantics: returns `newfd` on success
// and -1 with errno set on failure, even on Windows where _dup2 returns 0).
int Dup2(int oldfd, int newfd);

// Sequential write to `fd` (honours append mode if opened with append=true).
ssize_t Write(int fd, const void* buf, size_t count);

// Positional read/write without relying on the caller's stream state.
// Returns bytes transferred (0 at EOF for Pread), or -1 with errno set.
ssize_t Pread(int fd, void* buf, size_t count, int64_t offset);
ssize_t Pwrite(int fd, const void* buf, size_t count, int64_t offset);

// Clears O_APPEND on an open descriptor (no-op on Windows where Pwrite uses
// explicit OVERLAPPED offsets).
int ClearAppend(int fd);

// Resizes `fd` to `length` bytes (ftruncate / _chsize_s).
int Truncate(int fd, int64_t length);

// Queries the byte size of an open file descriptor (fstat / GetFileSizeEx).
int FileSize(int fd, int64_t* size_out);

// Non-blocking exclusive advisory lock on `fd` (flock(LOCK_EX|LOCK_NB) on
// POSIX; high-byte LockFileEx on Windows so normal file reads/writes on the
// same file are not blocked by mandatory byte-range checks). Sets
// errno = EWOULDBLOCK on contention.
int TryLockExclusive(int fd);
int UnlockFile(int fd);

// Flushes the directory entry metadata of `dir` to durable storage (open +
// fsync on POSIX; no-op returning 0 on Windows).
int SyncDirectory(const std::filesystem::path& dir);

// Makes the data (and the metadata needed to read it back) of `fd` durable.
// Linux: fdatasync(2). macOS: fcntl(F_FULLFSYNC), which, unlike fsync(2) on
// that platform, also flushes the drive's write cache; falls back to fsync(2)
// on file systems that reject F_FULLFSYNC. Windows: FlushFileBuffers.
// Retries EINTR. Returns 0 on success and -1 with errno set on failure.
int DataSync(int fd);

// Whether AdviseDontNeed actually reaches the kernel. Callers that would do
// extra work (opening files) only to issue the hint can skip it when false.
#ifdef __linux__
inline constexpr bool kSupportsAdviseDontNeed = true;
#else
inline constexpr bool kSupportsAdviseDontNeed = false;
#endif

// Best-effort hint that [offset, offset + length) of `fd` will not be read
// again soon (POSIX_FADV_DONTNEED). No-op where unsupported (macOS, Windows).
void AdviseDontNeed(int fd, int64_t offset, int64_t length);

// Reserves backing storage for [0, size) of `fd` so later writes in that
// range cannot fail with ENOSPC. Returns 0 on success, including when the
// platform or file system has no preallocation primitive (the caller then
// falls back to writing the range explicitly); returns -1 with errno set on a
// real failure.
int Preallocate(int fd, int64_t size);

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_FILE_IO_HPP
