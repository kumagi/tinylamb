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

namespace tinylamb::platform {

// Makes the data (and the metadata needed to read it back) of `fd` durable.
// Linux: fdatasync(2). macOS: fcntl(F_FULLFSYNC), which, unlike fsync(2) on
// that platform, also flushes the drive's write cache; falls back to fsync(2)
// on file systems that reject F_FULLFSYNC. Retries EINTR. Returns 0 on success
// and -1 with errno set on failure. Callers must check the result: silently
// skipping a sync would break the durability guarantee.
int DataSync(int fd);

// Whether AdviseDontNeed actually reaches the kernel. Callers that would do
// extra work (opening files) only to issue the hint can skip it when false.
#if defined(__linux__)
inline constexpr bool kSupportsAdviseDontNeed = true;
#else
inline constexpr bool kSupportsAdviseDontNeed = false;
#endif

// Best-effort hint that [offset, offset + length) of `fd` will not be read
// again soon (POSIX_FADV_DONTNEED). No-op where unsupported (macOS).
void AdviseDontNeed(int fd, off_t offset, off_t length);

// Reserves backing storage for [0, size) of `fd` so later writes in that
// range cannot fail with ENOSPC. Returns 0 on success, including when the
// platform or file system has no preallocation primitive (the caller then
// falls back to writing the range explicitly); returns -1 with errno set on a
// real failure.
int Preallocate(int fd, off_t size);

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_FILE_IO_HPP
