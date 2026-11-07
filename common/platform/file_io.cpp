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
#include <unistd.h>

#include <cerrno>

namespace tinylamb::platform {

int DataSync(int fd) {
  int rc = 0;
#if defined(__APPLE__)
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
#else
  do {
    rc = ::fdatasync(fd);
  } while (rc < 0 && errno == EINTR);
#endif
  return rc;
}

void AdviseDontNeed(int fd, off_t offset, off_t length) {
#if defined(__linux__)
  (void)::posix_fadvise(fd, offset, length, POSIX_FADV_DONTNEED);
#else
  (void)fd;
  (void)offset;
  (void)length;
#endif
}

int Preallocate(int fd, off_t size) {
#if defined(__linux__)
  int allocated = 0;
  do {
    allocated = ::posix_fallocate(fd, 0, size);
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
