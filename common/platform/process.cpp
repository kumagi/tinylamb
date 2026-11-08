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

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "common/platform/os.hpp"

#if defined(TINYLAMB_OS_WINDOWS)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <io.h>
#include <process.h>
#include <windows.h>
#elif defined(__APPLE__)
#include <crt_externs.h>
#include <mach-o/dyld.h>
#include <sys/wait.h>
#include <unistd.h>
#else
#include <sys/wait.h>
#include <unistd.h>
extern "C" {
extern char** environ;  // NOLINT(readability-redundant-declaration)
}
#endif

namespace tinylamb::platform {

std::filesystem::path CurrentExecutablePath() {
#if defined(TINYLAMB_OS_WINDOWS)
  std::array<wchar_t, 4096> buf{};
  const DWORD len =
      ::GetModuleFileNameW(nullptr, buf.data(), static_cast<DWORD>(buf.size()));
  if (len == 0 || len >= buf.size()) {
    return {};
  }
  return std::filesystem::path(std::wstring(buf.data(), len));
#elif defined(__APPLE__)
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
#if defined(TINYLAMB_OS_WINDOWS)
  return _environ;
#elif defined(__APPLE__)
  return *::_NSGetEnviron();
#else
  return environ;
#endif
}

int GetPid() {
#if defined(TINYLAMB_OS_WINDOWS)
  return ::_getpid();
#else
  return static_cast<int>(::getpid());
#endif
}

int SetEnv(const char* name, const char* value, int overwrite) {
#if defined(TINYLAMB_OS_WINDOWS)
  if (name == nullptr || *name == '\0' || std::strchr(name, '=') != nullptr ||
      value == nullptr) {
    errno = EINVAL;
    return -1;
  }
  if (overwrite == 0 && std::getenv(name) != nullptr) {
    return 0;
  }
  const errno_t err = ::_putenv_s(name, value);
  if (err != 0) {
    errno = err;
    return -1;
  }
  ::SetEnvironmentVariableA(name, value);
  return 0;
#else
  return ::setenv(name, value, overwrite);
#endif
}

int UnsetEnv(const char* name) {
#if defined(TINYLAMB_OS_WINDOWS)
  if (name == nullptr || *name == '\0' || std::strchr(name, '=') != nullptr) {
    errno = EINVAL;
    return -1;
  }
  const errno_t err = ::_putenv_s(name, "");
  if (err != 0) {
    errno = err;
    return -1;
  }
  ::SetEnvironmentVariableA(name, nullptr);
  return 0;
#else
  return ::unsetenv(name);
#endif
}

int RunProcess(
    const std::filesystem::path& executable,
    const std::vector<std::string>& args,
    const std::vector<std::pair<std::string, std::string>>& env_overrides,
    std::string* error_out) {
#if defined(TINYLAMB_OS_WINDOWS)
  auto quote_arg = [](std::wstring_view arg) -> std::wstring {
    if (!arg.empty() &&
        arg.find_first_of(L" \t\n\v\"") == std::wstring_view::npos) {
      return std::wstring(arg);
    }
    std::wstring out = L"\"";
    for (size_t i = 0; i < arg.size(); ++i) {
      size_t backslashes = 0;
      while (i < arg.size() && arg[i] == L'\\') {
        ++i;
        ++backslashes;
      }
      if (i == arg.size()) {
        out.append(backslashes * 2, L'\\');
        break;
      }
      if (arg[i] == L'"') {
        out.append(backslashes * 2 + 1, L'\\');
        out.push_back(L'"');
      } else {
        out.append(backslashes, L'\\');
        out.push_back(arg[i]);
      }
    }
    out.push_back(L'"');
    return out;
  };

  std::wstring cmd = quote_arg(executable.wstring());
  for (const std::string& a : args) {
    cmd.push_back(L' ');
    cmd.append(quote_arg(std::filesystem::path(a).wstring()));
  }

  std::vector<std::pair<std::string, std::string>> saved_env;
  for (const auto& [k, v] : env_overrides) {
    const char* prev = std::getenv(k.c_str());
    saved_env.emplace_back(k,
                           prev != nullptr ? std::string(prev) : std::string());
    if (prev == nullptr) {
      saved_env.back().first.insert(0, 1, '\0');
    }
    SetEnv(k.c_str(), v.c_str(), 1);
  }

  STARTUPINFOW si{};
  si.cb = sizeof(si);
  PROCESS_INFORMATION pi{};
  const BOOL ok =
      ::CreateProcessW(executable.c_str(), cmd.data(), nullptr, nullptr, FALSE,
                       0, nullptr, nullptr, &si, &pi);

  for (const auto& [k, v] : saved_env) {
    if (!k.empty() && k[0] == '\0') {
      UnsetEnv(k.c_str() + 1);
    } else {
      SetEnv(k.c_str(), v.c_str(), 1);
    }
  }

  if (!ok) {
    if (error_out != nullptr) {
      *error_out = "CreateProcessW failed for " + executable.string();
    }
    return -1;
  }
  ::WaitForSingleObject(pi.hProcess, INFINITE);
  DWORD exit_code = 1;
  ::GetExitCodeProcess(pi.hProcess, &exit_code);
  ::CloseHandle(pi.hThread);
  ::CloseHandle(pi.hProcess);
  return static_cast<int>(exit_code);
#else
  const pid_t child = ::fork();
  if (child < 0) {
    if (error_out != nullptr) {
      *error_out = "fork failed";
    }
    return -1;
  }
  if (child == 0) {
    for (const auto& [k, v] : env_overrides) {
      ::setenv(k.c_str(), v.c_str(), 1);
    }
    std::string exe_str = executable.string();
    std::vector<std::string> owned = args;
    std::vector<char*> argv;
    argv.reserve(owned.size() + 2);
    argv.push_back(exe_str.data());
    for (std::string& s : owned) {
      argv.push_back(s.data());
    }
    argv.push_back(nullptr);
    ::execv(exe_str.c_str(), argv.data());
    ::_exit(127);
  }
  int status = 0;
  while (::waitpid(child, &status, 0) < 0) {
    if (errno != EINTR) {
      if (error_out != nullptr) {
        *error_out = "waitpid failed";
      }
      return -1;
    }
  }
  if (!WIFEXITED(status)) {
    if (error_out != nullptr) {
      *error_out = "process terminated abnormally";
    }
    return -1;
  }
  return WEXITSTATUS(status);
#endif
}

int OpenWriteFailingDescriptor() {
#if defined(TINYLAMB_OS_WINDOWS)
  std::array<wchar_t, MAX_PATH + 1> temp_dir{};
  const DWORD dir_len =
      ::GetTempPathW(static_cast<DWORD>(temp_dir.size()), temp_dir.data());
  if (dir_len == 0 || dir_len >= temp_dir.size()) {
    errno = EIO;
    return -1;
  }
  std::array<wchar_t, MAX_PATH + 1> temp_file{};
  if (::GetTempFileNameW(temp_dir.data(), L"tlw", 0, temp_file.data()) == 0) {
    errno = EIO;
    return -1;
  }
  SECURITY_ATTRIBUTES sa{};
  sa.nLength = sizeof(sa);
  sa.bInheritHandle = FALSE;
  HANDLE h = ::CreateFileW(
      temp_file.data(), GENERIC_READ,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, &sa,
      OPEN_EXISTING, FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE,
      nullptr);
  if (h == INVALID_HANDLE_VALUE) {
    ::DeleteFileW(temp_file.data());
    errno = EIO;
    return -1;
  }
  const int fd = ::_open_osfhandle(reinterpret_cast<intptr_t>(h),
                                   _O_RDONLY | _O_BINARY | _O_NOINHERIT);
  if (fd < 0) {
    ::CloseHandle(h);
    errno = EMFILE;
    return -1;
  }
  return fd;
#elif defined(__linux__)
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
