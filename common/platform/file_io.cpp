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
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <string>
#include <system_error>
#include <vector>

#include "common/platform/os.hpp"

#ifdef TINYLAMB_OS_WINDOWS
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
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace tinylamb::platform {

#ifdef TINYLAMB_OS_WINDOWS
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
#ifdef TINYLAMB_OS_WINDOWS
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
#ifdef TINYLAMB_OS_WINDOWS
  return ::_close(fd);
#else
  return ::close(fd);
#endif
}

int OpenUnlinkedTempFile(const std::filesystem::path& dir) {
  std::filesystem::path target_dir = dir;
  if (target_dir.empty()) {
    std::error_code ec;
    target_dir = std::filesystem::temp_directory_path(ec);
    if (ec || target_dir.empty()) {
      target_dir = ".";
    }
  }
#ifdef TINYLAMB_OS_WINDOWS
  static std::atomic<uint64_t> counter{0};
  const DWORD pid = ::GetCurrentProcessId();
  const uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
  const std::filesystem::path tmp_path =
      target_dir / ("tinylamb_tmp_" + std::to_string(pid) + "_" +
                    std::to_string(seq) + ".tmp");
  SECURITY_ATTRIBUTES sa{};
  sa.nLength = sizeof(sa);
  sa.lpSecurityDescriptor = nullptr;
  sa.bInheritHandle = FALSE;
  HANDLE h = ::CreateFileW(
      tmp_path.c_str(), GENERIC_READ | GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, &sa, CREATE_NEW,
      FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE, nullptr);
  if (h == INVALID_HANDLE_VALUE) {
    errno = MapWin32ErrorToErrno(::GetLastError());
    return -1;
  }
  const int fd = ::_open_osfhandle(reinterpret_cast<intptr_t>(h),
                                   _O_BINARY | _O_NOINHERIT);
  if (fd < 0) {
    ::CloseHandle(h);
    errno = EMFILE;
    return -1;
  }
  return fd;
#else
#if defined(__linux__) && defined(O_TMPFILE)
  const int tmp_fd =
      ::open(target_dir.c_str(), O_TMPFILE | O_RDWR | O_CLOEXEC, 0600);
  if (tmp_fd >= 0) {
    return tmp_fd;
  }
  // T2629: keep the true failure reason of the O_TMPFILE path; a fallback
  // that fails for its own unrelated reason must not mask e.g. fd exhaustion.
  const int tmpfile_errno = errno;
#endif
  std::string tmpl = (target_dir / "tinylamb_tmp_XXXXXX").string();
  const int fd = ::mkstemp(tmpl.data());
  if (fd < 0) {
#if defined(__linux__) && defined(O_TMPFILE)
    if (tmpfile_errno != EOPNOTSUPP && tmpfile_errno != EISDIR &&
        tmpfile_errno != ENOTDIR && tmpfile_errno != EINVAL &&
        tmpfile_errno != ENOSYS) {
      errno = tmpfile_errno;
    }
#endif
    return -1;
  }
  // T2623: the header promises a non-inheritable descriptor (the O_TMPFILE
  // path passes O_CLOEXEC, Windows passes _O_NOINHERIT); a failed
  // F_SETFD must fail the call, not leak an inheritable temp fd.
  if (::fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
    const int saved = errno;
    (void)::close(fd);
    ::remove(tmpl.c_str());  // NOLINT(cert-err33-c)
    errno = saved;
    return -1;
  }
  if (::unlink(tmpl.c_str()) != 0) {
    const int saved = errno;
    (void)::close(fd);
    // T2620: the contract is "no name survives" -- on unlink failure the
    // mkstemp file would linger forever (tmpl dies with this frame, so no
    // one else can remove it). Best-effort remove before reporting.
    ::remove(tmpl.c_str());  // NOLINT(cert-err33-c)
    errno = saved != 0 ? saved : EIO;
    return -1;
  }
  return fd;
#endif
}

std::FILE* FdOpenStream(int fd, const char* mode) {
  if (fd < 0) {
    return nullptr;
  }
#ifdef TINYLAMB_OS_WINDOWS
  std::FILE* file = ::_fdopen(fd, mode);
#else
  std::FILE* file = ::fdopen(fd, mode);
#endif
  if (file == nullptr) {
    const int saved = errno;
    (void)CloseFile(fd);
    errno = saved;
  }
  return file;
}

int Dup2(int oldfd, int newfd) {
#ifdef TINYLAMB_OS_WINDOWS
  return ::_dup2(oldfd, newfd) == 0 ? newfd : -1;
#else
  return ::dup2(oldfd, newfd);
#endif
}

int Dup(int fd) {
#ifdef TINYLAMB_OS_WINDOWS
  return ::_dup(fd);
#else
  return ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
#endif
}

ssize_t Write(int fd, const void* buf, size_t count) {
#ifdef TINYLAMB_OS_WINDOWS
  // _write takes an unsigned int byte count. Loop so a bulk write larger than
  // INT_MAX is not silently truncated to its first chunk (POSIX ::write gets
  // the full size_t count in one call).
  const auto* bytes = static_cast<const unsigned char*>(buf);
  size_t written = 0;
  while (written < count) {
    const auto capped = static_cast<unsigned int>(
        std::min<size_t>(count - written, static_cast<size_t>(INT_MAX)));
    const int r = ::_write(fd, bytes + written, capped);
    if (r < 0) {
      if (errno == EACCES) {
        errno = EBADF;
      }
      // Match POSIX: report partial progress rather than discarding it.
      return written == 0 ? -1 : static_cast<ssize_t>(written);
    }
    if (r == 0) {
      break;  // No progress: avoid spinning forever on a short write.
    }
    written += static_cast<size_t>(r);
  }
  return static_cast<ssize_t>(written);
#else
  return ::write(fd, buf, count);
#endif
}

ssize_t Pread(int fd, void* buf, size_t count, int64_t offset) {
#ifdef TINYLAMB_OS_WINDOWS
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
#ifdef TINYLAMB_OS_WINDOWS
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
#ifdef TINYLAMB_OS_WINDOWS
  (void)fd;
  return 0;
#else
  const int flags = ::fcntl(fd, F_GETFL);
  return flags < 0 ? -1 : ::fcntl(fd, F_SETFL, flags & ~O_APPEND);
#endif
}

int Truncate(int fd, int64_t length) {
#ifdef TINYLAMB_OS_WINDOWS
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
#ifdef TINYLAMB_OS_WINDOWS
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
#ifdef TINYLAMB_OS_WINDOWS
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
#ifdef TINYLAMB_OS_WINDOWS
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
#ifdef TINYLAMB_OS_WINDOWS
  // Flush the directory entry itself so a rename is durable across a power
  // loss. FILE_FLAG_BACKUP_SEMANTICS is required to open a directory handle.
  HANDLE h = ::CreateFileW(
      dir.c_str(), GENERIC_READ,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
      OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, nullptr);
  if (h == INVALID_HANDLE_VALUE) {
    errno = MapWin32ErrorToErrno(::GetLastError());
    return -1;
  }
  const BOOL ok = ::FlushFileBuffers(h);
  const DWORD err = ok ? 0 : ::GetLastError();
  ::CloseHandle(h);
  if (!ok) {
    errno = MapWin32ErrorToErrno(err);
    return -1;
  }
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
#ifdef TINYLAMB_OS_WINDOWS
  const intptr_t raw = ::_get_osfhandle(fd);
  if (raw == -1) {
    errno = EBADF;
    return -1;
  }
  if (!::FlushFileBuffers(reinterpret_cast<HANDLE>(raw))) {
    const DWORD err = ::GetLastError();
    if (err == ERROR_ACCESS_DENIED) {
      // Windows cannot FlushFileBuffers through a read-only handle. Recover
      // the file's full path and flush via a writable handle so callers still
      // get a real durability barrier (matching POSIX fdatasync on O_RDONLY).
      // GetFinalPathNameByHandleW returns a DOS path including the drive
      // letter; FILE_NAME_INFO's volume-relative name resolves against the
      // current drive and would target the wrong file (or fail) when the DB
      // lives on a different drive than the CWD.
      const HANDLE handle = reinterpret_cast<HANDLE>(raw);
      std::wstring path;
      const DWORD path_chars = ::GetFinalPathNameByHandleW(
          handle, nullptr, 0, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
      if (path_chars > 0) {
        path.resize(path_chars);
        const DWORD written =
            ::GetFinalPathNameByHandleW(handle, path.data(), path_chars,
                                        FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
        if (written > 0 && written < path_chars) {
          path.resize(written);
        } else {
          path.clear();
        }
      }
      if (!path.empty()) {
        HANDLE writable = ::CreateFileW(
            path.c_str(), GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
            OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (writable != INVALID_HANDLE_VALUE) {
          const BOOL flushed = ::FlushFileBuffers(writable);
          const DWORD flush_err = flushed ? 0 : ::GetLastError();
          ::CloseHandle(writable);
          if (flushed) {
            return 0;
          }
          errno = MapWin32ErrorToErrno(flush_err);
          return -1;
        }
      }
      errno = EACCES;
      return -1;
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
  // Only "filesystem does not support fallocate" is benign. EINVAL means an
  // invalid size/offset request and must surface as an error.
  if (allocated != 0 && allocated != EOPNOTSUPP && allocated != ENOSYS) {
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

namespace {
constexpr uint8_t kEmptyMapByte = 0;

#ifndef TINYLAMB_OS_WINDOWS
void AdviseMemoryRange(const uint8_t* base, size_t map_size, size_t offset,
                       size_t length, int advice) noexcept {
  if (base == nullptr || map_size == 0 || offset >= map_size || length == 0) {
    return;
  }
  const size_t clamped_len = std::min(length, map_size - offset);
  const long raw_page = ::sysconf(_SC_PAGESIZE);
  const size_t page_size =
      raw_page > 0 ? static_cast<size_t>(raw_page) : static_cast<size_t>(4096);
  const size_t aligned_offset = offset & ~(page_size - 1);
  // clamped_len already ends at or before map_size, so extending the start
  // down to the page boundary keeps the advised range inside the mapping:
  // aligned_offset + aligned_len == offset + clamped_len <= map_size.
  const size_t aligned_end = offset + clamped_len;
  const size_t aligned_len = aligned_end - aligned_offset;
  // POSIX madvise takes non-const void* even for read-only mappings.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  (void)::madvise(const_cast<uint8_t*>(base + aligned_offset), aligned_len,
                  advice);
}
#endif
}  // namespace

MemoryMap::MemoryMap(MemoryMap&& other) noexcept
    : data_(other.data_),
      size_(other.size_),
      valid_(other.valid_)
#ifdef TINYLAMB_OS_WINDOWS
      ,
      mapping_handle_(other.mapping_handle_)
#endif
{
  other.data_ = nullptr;
  other.size_ = 0;
  other.valid_ = false;
#ifdef TINYLAMB_OS_WINDOWS
  other.mapping_handle_ = nullptr;
#endif
}

MemoryMap& MemoryMap::operator=(MemoryMap&& other) noexcept {
  if (this != &other) {
    Close();
    data_ = other.data_;
    size_ = other.size_;
    valid_ = other.valid_;
    other.data_ = nullptr;
    other.size_ = 0;
    other.valid_ = false;
#ifdef TINYLAMB_OS_WINDOWS
    mapping_handle_ = other.mapping_handle_;
    other.mapping_handle_ = nullptr;
#endif
  }
  return *this;
}

MemoryMap::~MemoryMap() { Close(); }

int MemoryMap::Open(const std::filesystem::path& path) {
  Close();
  const int fd = OpenFile(path, {.mode = OpenMode::kReadOnly});
  if (fd < 0) {
    return -1;
  }
  int64_t file_size = 0;
  if (FileSize(fd, &file_size) != 0 || file_size < 0) {
    const int saved = errno;
    (void)CloseFile(fd);
    errno = saved != 0 ? saved : EIO;
    return -1;
  }
  if (static_cast<uint64_t>(file_size) >
      static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    (void)CloseFile(fd);
    errno = EOVERFLOW;
    return -1;
  }
  if (file_size == 0) {
    (void)CloseFile(fd);
    data_ = &kEmptyMapByte;
    size_ = 0;
    valid_ = true;
    return 0;
  }
  const auto map_size = static_cast<size_t>(file_size);
#ifdef TINYLAMB_OS_WINDOWS
  const intptr_t raw = ::_get_osfhandle(fd);
  if (raw == -1) {
    (void)CloseFile(fd);
    errno = EBADF;
    return -1;
  }
  HANDLE mapping = ::CreateFileMappingW(reinterpret_cast<HANDLE>(raw), nullptr,
                                        PAGE_READONLY, 0, 0, nullptr);
  if (mapping == nullptr) {
    const DWORD err = ::GetLastError();
    (void)CloseFile(fd);
    errno = MapWin32ErrorToErrno(err);
    return -1;
  }
  void* view = ::MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, map_size);
  (void)CloseFile(fd);
  if (view == nullptr) {
    const DWORD err = ::GetLastError();
    ::CloseHandle(mapping);
    errno = MapWin32ErrorToErrno(err);
    return -1;
  }
  mapping_handle_ = mapping;
  data_ = static_cast<const uint8_t*>(view);
  size_ = map_size;
  valid_ = true;
  return 0;
#else
  void* addr = ::mmap(nullptr, map_size, PROT_READ, MAP_PRIVATE, fd, 0);
  const int saved = errno;
  (void)CloseFile(fd);
  if (addr == MAP_FAILED) {
    errno = saved;
    return -1;
  }
  data_ = static_cast<const uint8_t*>(addr);
  size_ = map_size;
  valid_ = true;
  return 0;
#endif
}

void MemoryMap::Close() noexcept {
  if (!valid_) {
    return;
  }
  if (size_ > 0 && data_ != nullptr) {
#ifdef TINYLAMB_OS_WINDOWS
    (void)::UnmapViewOfFile(data_);
    if (mapping_handle_ != nullptr) {
      ::CloseHandle(static_cast<HANDLE>(mapping_handle_));
      mapping_handle_ = nullptr;
    }
#else
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    (void)::munmap(const_cast<uint8_t*>(data_), size_);
#endif
  }
  data_ = nullptr;
  size_ = 0;
  valid_ = false;
}

void MemoryMap::AdviseSequential(size_t offset, size_t length) const noexcept {
  if (!valid_) {
    return;
  }
#ifdef TINYLAMB_OS_WINDOWS
  (void)offset;
  (void)length;
#else
  AdviseMemoryRange(data_, size_, offset, length, MADV_SEQUENTIAL);
#endif
}

void MemoryMap::AdviseWillNeed(size_t offset, size_t length) const noexcept {
  if (!valid_) {
    return;
  }
#ifdef TINYLAMB_OS_WINDOWS
  (void)offset;
  (void)length;
#else
  AdviseMemoryRange(data_, size_, offset, length, MADV_WILLNEED);
#endif
}

void MemoryMap::AdviseRandom(size_t offset, size_t length) const noexcept {
  if (!valid_) {
    return;
  }
#ifdef TINYLAMB_OS_WINDOWS
  (void)offset;
  (void)length;
#else
  AdviseMemoryRange(data_, size_, offset, length, MADV_RANDOM);
#endif
}

}  // namespace tinylamb::platform
