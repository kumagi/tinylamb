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

#ifndef TINYLAMB_COMMON_PLATFORM_SYNCED_STREAM_HPP
#define TINYLAMB_COMMON_PLATFORM_SYNCED_STREAM_HPP

#include <version>

#ifdef __cpp_lib_syncbuf
#include <syncstream>
#else
#include <mutex>
#include <ostream>
#include <sstream>
#endif

namespace tinylamb::platform {

#ifdef __cpp_lib_syncbuf

// C++20 std::osyncstream where the standard library provides it.
using SyncedOStream = std::osyncstream;

#else

// Fallback for standard libraries without <syncstream> (Apple libc++):
// buffers everything written through it and emits the whole buffer to the
// wrapped stream under one process-wide lock on destruction, so lines from
// concurrent threads do not interleave. Same usage as std::osyncstream:
//   SyncedOStream(std::cerr) << "a=" << a << '\n';
class SyncedOStream : public std::ostream {
 public:
  explicit SyncedOStream(std::ostream& target)
      : std::ostream(nullptr), target_(target) {
    rdbuf(&buffer_);
  }
  SyncedOStream(const SyncedOStream&) = delete;
  SyncedOStream& operator=(const SyncedOStream&) = delete;
  ~SyncedOStream() override {
    std::scoped_lock lock(Mutex());
    target_ << buffer_.str();
    target_.flush();
  }

 private:
  static std::mutex& Mutex() {
    static std::mutex mu;
    return mu;
  }

  std::ostream& target_;
  std::stringbuf buffer_;
};

#endif

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_SYNCED_STREAM_HPP
