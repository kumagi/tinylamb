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

#include "common/platform/process.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <system_error>

#ifdef __APPLE__
#include <crt_externs.h>
#include <mach-o/dyld.h>
#else
extern "C" {
extern char** environ;  // NOLINT(readability-redundant-declaration)
}
#endif

namespace tinylamb::platform {

std::filesystem::path CurrentExecutablePath() {
#ifdef __APPLE__
  uint32_t size = 0;
  (void)::_NSGetExecutablePath(nullptr, &size);
  std::string buffer(size, '\0');
  if (size == 0 || ::_NSGetExecutablePath(buffer.data(), &size) != 0) {
    return {};
  }
  buffer.resize(std::char_traits<char>::length(buffer.c_str()));
  std::error_code ec;
  std::filesystem::path resolved = std::filesystem::canonical(buffer, ec);
  return ec ? std::filesystem::path(buffer) : resolved;
#else
  std::array<char, 4096> self{};
  const ssize_t len =
      ::readlink("/proc/self/exe", self.data(), self.size() - 1);
  if (len <= 0) {
    return {};
  }
  return {std::string(self.data(), static_cast<size_t>(len))};
#endif
}

char** Environment() {
#ifdef __APPLE__
  return *::_NSGetEnviron();
#else
  return environ;
#endif
}

int OpenWriteFailingDescriptor() {
#ifdef __linux__
  return ::open("/dev/full", O_WRONLY | O_CLOEXEC);
#else
  // No /dev/full: a read-only descriptor on a scratch file rejects every
  // write with EBADF while staying open (and fsync-able).
  std::string pattern =
      (std::filesystem::temp_directory_path() / "tinylamb_wfail_XXXXXX")
          .string();
  const int scratch = ::mkstemp(pattern.data());
  if (scratch < 0) return -1;
  ::close(scratch);
  const int fd = ::open(pattern.c_str(), O_RDONLY | O_CLOEXEC);
  const int saved = errno;
  ::unlink(pattern.c_str());
  errno = saved;
  return fd;
#endif
}

}  // namespace tinylamb::platform
