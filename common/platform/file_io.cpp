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

#include "common/platform/file_io.hpp"

#include <fcntl.h>

#include <algorithm>
#include <cerrno>
#include <limits>

#include "common/platform/os.hpp"

#if defined(TINYLAMB_OS_WINDOWS)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <io.h>
#include <windows.h>
#else
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace tinylamb::platform {

#if defined(TINYLAMB_OS_WINDOWS)
namespace {

int MapWin32ErrorToErrno(DWORD err) {
  switch (err) {
    case ERROR_FILE_NOT_FOUND:
    case ERROR_PATH_NOT_FOUND:
      return ENOENT;
    case ERROR_ACCESS_DENIED:
    case ERROR_SHARING_VIOLATION:
      return EACCES;
    case ERROR_LOCK_VIOLATION:
      return EWOULDBLOCK;
    case ERROR_FILE_EXISTS:
    case ERROR_ALREADY_EXISTS:
      return EEXIST;
    case ERROR_TOO_MANY_OPEN_FILES:
      return EMFILE;
    case ERROR_INVALID_HANDLE:
      return EBADF;
    case ERROR_DISK_FULL:
    case ERROR_HANDLE_DISK_FULL:
      return ENOSPC;
    case ERROR_INVALID_PARAMETER:
      return EINVAL;
    default:
      return EIO;
  }
}

constexpr DWORD kLockByteLow = 0x00000000U;
constexpr DWORD kLockByteHigh = 0x7fffffffU;

}  // namespace
#endif

int OpenFile(const std::filesystem::path& path, OpenFlags flags, int mode) {
#if defined(TINYLAMB_OS_WINDOWS)
  (void)mode;
  const bool writable = flags.mode == OpenMode::kReadWrite;
  const DWORD desired_access =
      writable ? (GENERIC_READ | GENERIC_WRITE) : GENERIC_READ;
  const DWORD share_mode =
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
  SECURITY_ATTRIBUTES sa{};
  sa.nLength = sizeof(sa);
  sa.lpSecurityDescriptor = nullptr;
  sa.bInheritHandle = FALSE;

  DWORD creation = OPEN_EXISTING;
  if (flags.create && flags.truncate) {
    creation = CREATE_ALWAYS;
  } else if (flags.create) {
    creation = OPEN_ALWAYS;
  } else if (flags.truncate) {
    creation = TRUNCATE_EXISTING;
  }

  HANDLE h = ::CreateFileW(path.c_str(), desired_access, share_mode, &sa,
                           creation, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (h == INVALID_HANDLE_VALUE) {
    errno = MapWin32ErrorToErrno(::GetLastError());
    return -1;
  }
  int crt_flags = _O_BINARY | _O_NOINHERIT;
  if (!writable) {
    crt_flags |= _O_RDONLY;
  }
  if (flags.append) {
    crt_flags |= _O_APPEND;
  }
  const int fd = ::_open_osfhandle(reinterpret_cast<intptr_t>(h), crt_flags);
  if (fd < 0) {
    ::CloseHandle(h);
    errno = EMFILE;
    return -1;
  }
  return fd;
#else
  int os_flags = O_CLOEXEC;
  os_flags |= (flags.mode == OpenMode::kReadWrite) ? O_RDWR : O_RDONLY;
  if (flags.create) {
    os_flags |= O_CREAT;
  }
  if (flags.append) {
    os_flags |= O_APPEND;
  }
  if (flags.truncate) {
    os_flags |= O_TRUNC;
  }
  return ::open(path.c_str(), os_flags, mode);
#endif
}

int CloseFile(int fd) {
#if defined(TINYLAMB_OS_WINDOWS)
  return ::_close(fd);
#else
  return ::close(fd);
#endif
}

int Dup2(int oldfd, int newfd) {
#if defined(TINYLAMB_OS_WINDOWS)
  return ::_dup2(oldfd, newfd) == 0 ? newfd : -1;
#else
  return ::dup2(oldfd, newfd);
#endif
}

ssize_t Write(int fd, const void* buf, size_t count) {
#if defined(TINYLAMB_OS_WINDOWS)
  const auto capped = static_cast<unsigned int>(
      std::min<size_t>(count, static_cast<size_t>(INT_MAX)));
  const int r = ::_write(fd, buf, capped);
  if (r < 0 && errno == EACCES) {
    errno = EBADF;
  }
  return r;
#else
  return ::write(fd, buf, count);
#endif
}

ssize_t Pread(int fd, void* buf, size_t count, int64_t offset) {
#if defined(TINYLAMB_OS_WINDOWS)
  if (offset < 0) {
    errno = EINVAL;
    return -1;
  }
  const intptr_t raw = ::_get_osfhandle(fd);
  if (raw == -1) {
    errno = EBADF;
    return -1;
  }
  auto* h = reinterpret_cast<HANDLE>(raw);
  OVERLAPPED ov{};
  const auto uoff = static_cast<uint64_t>(offset);
  ov.Offset = static_cast<DWORD>(uoff & 0xffffffffULL);
  ov.OffsetHigh = static_cast<DWORD>(uoff >> 32);
  const auto chunk = static_cast<DWORD>(
      std::min<size_t>(count, static_cast<size_t>(0x7fffffffU)));
  DWORD bytes_read = 0;
  if (!::ReadFile(h, buf, chunk, &bytes_read, &ov)) {
    const DWORD err = ::GetLastError();
    if (err == ERROR_HANDLE_EOF) {
      return 0;
    }
    errno = MapWin32ErrorToErrno(err);
    return -1;
  }
  return static_cast<ssize_t>(bytes_read);
#else
  return ::pread(fd, buf, count, static_cast<off_t>(offset));
#endif
}

ssize_t Pwrite(int fd, const void* buf, size_t count, int64_t offset) {
#if defined(TINYLAMB_OS_WINDOWS)
  if (offset < 0) {
    errno = EINVAL;
    return -1;
  }
  const intptr_t raw = ::_get_osfhandle(fd);
  if (raw == -1) {
    errno = EBADF;
    return -1;
  }
  auto* h = reinterpret_cast<HANDLE>(raw);
  OVERLAPPED ov{};
  const auto uoff = static_cast<uint64_t>(offset);
  ov.Offset = static_cast<DWORD>(uoff & 0xffffffffULL);
  ov.OffsetHigh = static_cast<DWORD>(uoff >> 32);
  const auto chunk = static_cast<DWORD>(
      std::min<size_t>(count, static_cast<size_t>(0x7fffffffU)));
  DWORD bytes_written = 0;
  if (!::WriteFile(h, buf, chunk, &bytes_written, &ov)) {
    const DWORD err = ::GetLastError();
    errno = (err == ERROR_ACCESS_DENIED) ? EBADF : MapWin32ErrorToErrno(err);
    return -1;
  }
  return static_cast<ssize_t>(bytes_written);
#else
  return ::pwrite(fd, buf, count, static_cast<off_t>(offset));
#endif
}

int ClearAppend(int fd) {
#if defined(TINYLAMB_OS_WINDOWS)
  (void)fd;
  return 0;
#else
  const int flags = ::fcntl(fd, F_GETFL);
  return flags < 0 ? -1 : ::fcntl(fd, F_SETFL, flags & ~O_APPEND);
#endif
}

int Truncate(int fd, int64_t length) {
#if defined(TINYLAMB_OS_WINDOWS)
  const errno_t err = ::_chsize_s(fd, length);
  if (err != 0) {
    errno = err;
    return -1;
  }
  return 0;
#else
  return ::ftruncate(fd, static_cast<off_t>(length));
#endif
}

int FileSize(int fd, int64_t* size_out) {
#if defined(TINYLAMB_OS_WINDOWS)
  const intptr_t raw = ::_get_osfhandle(fd);
  if (raw == -1) {
    errno = EBADF;
    return -1;
  }
  LARGE_INTEGER li{};
  if (!::GetFileSizeEx(reinterpret_cast<HANDLE>(raw), &li)) {
    errno = MapWin32ErrorToErrno(::GetLastError());
    return -1;
  }
  *size_out = static_cast<int64_t>(li.QuadPart);
  return 0;
#else
  struct stat st{};
  if (::fstat(fd, &st) != 0) {
    return -1;
  }
  *size_out = static_cast<int64_t>(st.st_size);
  return 0;
#endif
}

int TryLockExclusive(int fd) {
#if defined(TINYLAMB_OS_WINDOWS)
  const intptr_t raw = ::_get_osfhandle(fd);
  if (raw == -1) {
    errno = EBADF;
    return -1;
  }
  OVERLAPPED ov{};
  ov.Offset = kLockByteLow;
  ov.OffsetHigh = kLockByteHigh;
  if (!::LockFileEx(reinterpret_cast<HANDLE>(raw),
                    LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1,
                    0, &ov)) {
    errno = MapWin32ErrorToErrno(::GetLastError());
    return -1;
  }
  return 0;
#else
  return ::flock(fd, LOCK_EX | LOCK_NB);
#endif
}

int UnlockFile(int fd) {
#if defined(TINYLAMB_OS_WINDOWS)
  const intptr_t raw = ::_get_osfhandle(fd);
  if (raw == -1) {
    errno = EBADF;
    return -1;
  }
  OVERLAPPED ov{};
  ov.Offset = kLockByteLow;
  ov.OffsetHigh = kLockByteHigh;
  if (!::UnlockFileEx(reinterpret_cast<HANDLE>(raw), 0, 1, 0, &ov)) {
    errno = MapWin32ErrorToErrno(::GetLastError());
    return -1;
  }
  return 0;
#else
  return ::flock(fd, LOCK_UN);
#endif
}

int SyncDirectory(const std::filesystem::path& dir) {
#if defined(TINYLAMB_OS_WINDOWS)
  (void)dir;
  return 0;
#else
  const int dir_fd = ::open(dir.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECTORY);
  if (dir_fd < 0) {
    return -1;
  }
  int rc = 0;
  int saved = 0;
  do {
    rc = ::fsync(dir_fd);
    saved = errno;
  } while (rc != 0 && saved == EINTR);
  const int close_rc = ::close(dir_fd);
  if (rc != 0) {
    errno = saved;
    return -1;
  }
  if (close_rc != 0) {
    return -1;
  }
  return 0;
#endif
}

int DataSync(int fd) {
#if defined(TINYLAMB_OS_WINDOWS)
  const intptr_t raw = ::_get_osfhandle(fd);
  if (raw == -1) {
    errno = EBADF;
    return -1;
  }
  if (!::FlushFileBuffers(reinterpret_cast<HANDLE>(raw))) {
    const DWORD err = ::GetLastError();
    if (err == ERROR_ACCESS_DENIED) {
      // Read-only descriptor (e.g. WriteSegmentMetadata); the preceding writer
      // handle already flushed its CRT stream.
      return 0;
    }
    errno = MapWin32ErrorToErrno(err);
    return -1;
  }
  return 0;
#elif defined(__APPLE__)
  int rc = 0;
  do {
    rc = ::fcntl(fd, F_FULLFSYNC);
  } while (rc < 0 && errno == EINTR);
  if (rc < 0 && (errno == ENOTSUP || errno == EINVAL || errno == ENOTTY)) {
    // Some file systems (e.g. network or FUSE mounts) do not implement
    // F_FULLFSYNC; plain fsync is the strongest barrier they offer.
    do {
      rc = ::fsync(fd);
    } while (rc < 0 && errno == EINTR);
  }
  return rc;
#else
  int rc = 0;
  do {
    rc = ::fdatasync(fd);
  } while (rc < 0 && errno == EINTR);
  return rc;
#endif
}

void AdviseDontNeed(int fd, int64_t offset, int64_t length) {
#ifdef __linux__
  (void)::posix_fadvise(fd, static_cast<off_t>(offset),
                        static_cast<off_t>(length), POSIX_FADV_DONTNEED);
#else
  (void)fd;
  (void)offset;
  (void)length;
#endif
}

int Preallocate(int fd, int64_t size) {
#ifdef __linux__
  int allocated = 0;
  do {
    allocated = ::posix_fallocate(fd, 0, static_cast<off_t>(size));
  } while (allocated == EINTR);
  if (allocated != 0 && allocated != EOPNOTSUPP && allocated != ENOSYS &&
      allocated != EINVAL) {
    errno = allocated;
    return -1;
  }
  return 0;
#else
  // macOS has fcntl(F_PREALLOCATE), but it reserves blocks without extending
  // the file and is unsupported on several file systems; callers already
  // materialize the range with explicit writes, so treat it as unsupported.
  (void)fd;
  (void)size;
  return 0;
#endif
}

}  // namespace tinylamb::platform
