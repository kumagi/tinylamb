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

#ifndef TINYLAMB_COMMON_PLATFORM_PROCESS_HPP
#define TINYLAMB_COMMON_PLATFORM_PROCESS_HPP

#include <cerrno>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

namespace tinylamb::platform {

// Absolute path of the running executable (/proc/self/exe on Linux,
// _NSGetExecutablePath on macOS, GetModuleFileNameW on Windows), or an empty
// path if it cannot be found.
std::filesystem::path CurrentExecutablePath();

// The current process environment (`environ`), for posix_spawn/execve.
// macOS does not declare `environ` for code linked into shared objects and
// executables alike, so it is fetched through _NSGetEnviron() there.
char** Environment();

// Current process ID (getpid / _getpid).
int GetPid();

// Portable environment mutation (setenv/unsetenv on POSIX, _putenv_s +
// SetEnvironmentVariableA on Windows).
int SetEnv(const char* name, const char* value, int overwrite = 1);
int UnsetEnv(const char* name);

// Reads the current process environment. The returned pointer has the same
// lifetime rules as getenv: it may be invalidated by a later environment
// mutation. Use this together with SetEnv/UnsetEnv so reads and writes share
// the process-environment lock.
const char* GetEnv(const char* name);

// Returns an owned, thread-safe message for an errno value.
std::string StrError(int error);

// Spawns `executable` with `args` (excluding argv[0]) and `env_overrides`
// merged into the current environment, waits for completion, and returns the
// child's exit code (0 on success, non-zero exit code, or -1 if spawning
// failed).
int RunProcess(
    const std::filesystem::path& executable,
    const std::vector<std::string>& args,
    const std::vector<std::pair<std::string, std::string>>& env_overrides = {},
    std::string* error_out = nullptr);

// Opens a descriptor that stays valid but on which every write(2)/pwrite(2)
// fails (/dev/full -> ENOSPC on Linux; a read-only descriptor -> EBADF
// elsewhere). For fault-injection tests that dup2 it over a live file.
// Returns -1 with errno set on failure.
int OpenWriteFailingDescriptor();

// errno reported by writes to OpenWriteFailingDescriptor().
#ifdef __linux__
inline constexpr int kWriteFailingErrno = ENOSPC;
#else
inline constexpr int kWriteFailingErrno = EBADF;
#endif

// Returns the current resident set size (RSS) of this process in bytes, or 0
// if unavailable on the host OS.
uint64_t CurrentProcessRssBytes() noexcept;

// Returns the peak resident set size (high-water mark) of this process in
// bytes, or 0 if unavailable.
uint64_t PeakProcessRssBytes() noexcept;

// Asks the global allocator to return freed-but-retained heap pages to the OS
// (mi_collect(true) under mimalloc, malloc_trim(0) under glibc).  Resident
// memory that the allocator keeps after free() still counts toward RSS, so a
// budget that gates on RSS must be able to reclaim it before refusing a
// reservation.  Rate-limited (at most once per ~20 ms process-wide); returns
// true when a purge actually ran.  No-op on platforms without a hook.
bool ReleaseFreeHeapToOs() noexcept;

// Total physical memory available to this process in bytes, or 0 if
// unavailable.  On Linux this is min(host RAM, cgroup memory limit): a
// container must not size its budgets off the host's RAM (T2630).
uint64_t PhysicalMemoryBytes() noexcept;

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_PROCESS_HPP
