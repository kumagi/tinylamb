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
#if defined(__linux__)
inline constexpr int kWriteFailingErrno = ENOSPC;
#else
inline constexpr int kWriteFailingErrno = EBADF;
#endif

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_PROCESS_HPP
