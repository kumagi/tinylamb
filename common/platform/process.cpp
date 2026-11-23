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

#include <limits>
#if defined(__linux__) && defined(__GLIBC__)
#include <malloc.h>
#endif
#if __has_include(<mimalloc.h>)
#include <mimalloc.h>
#define TINYLAMB_HAVE_MIMALLOC_COLLECT 1
#endif

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/platform/os.hpp"
#include "common/platform/socket.hpp"

#ifdef TINYLAMB_OS_WINDOWS
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <io.h>
#include <process.h>
#include <psapi.h>
#include <windows.h>
#elif defined(__APPLE__)
#include <crt_externs.h>
#include <mach-o/dyld.h>
#include <mach/mach.h>
#include <spawn.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#else
#include <spawn.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
extern "C" {
extern char** environ;  // NOLINT(readability-redundant-declaration)
}
#endif

namespace tinylamb::platform {

std::filesystem::path CurrentExecutablePath() {
#ifdef TINYLAMB_OS_WINDOWS
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
  // T625: readlink(2) does not NUL-terminate and reports the length used;
  // a fixed buffer truncated longer install paths SILENTLY (len == size-1
  // on the old code, and the truncated path then broke relative resource
  // resolution). Grow until the result no longer fills the buffer.
  std::vector<char> self(512);
  for (;;) {
    const ssize_t len = ::readlink("/proc/self/exe", self.data(), self.size());
    if (len <= 0) {
      return {};
    }
    if (static_cast<size_t>(len) < self.size()) {
      return {std::string(self.data(), static_cast<size_t>(len))};
    }
    self.resize(self.size() * 2);
  }
#endif
}

char** Environment() {
#ifdef TINYLAMB_OS_WINDOWS
  return _environ;
#elif defined(__APPLE__)
  return *::_NSGetEnviron();
#else
  return environ;
#endif
}

int GetPid() {
#ifdef TINYLAMB_OS_WINDOWS
  return ::_getpid();
#else
  return static_cast<int>(::getpid());
#endif
}

int SetEnv(const char* name, const char* value, int overwrite) {
#ifdef TINYLAMB_OS_WINDOWS
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
#ifdef TINYLAMB_OS_WINDOWS
  if (name == nullptr || *name == '\0' || std::strchr(name, '=') != nullptr) {
    errno = EINVAL;
    return -1;
  }
  // The CRT removes a variable when its value string is empty. A null value
  // pointer invokes the invalid-parameter handler.
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
  // The OS process APIs use NUL-terminated keys and values.
  for (const auto& [key, value] : env_overrides) {
    if (key.empty() || key.find('=') != std::string::npos ||
        key.find('\0') != std::string::npos ||
        value.find('\0') != std::string::npos) {
      if (error_out != nullptr) {
        *error_out = "invalid environment override";
      }
      return -1;
    }
  }
#ifdef TINYLAMB_OS_WINDOWS
  // CreateProcessW takes NUL-terminated strings. A path/argument with an
  // embedded NUL would be silently truncated at c_str(), launching a
  // different program than the caller asked for (POSIX rejects the same
  // input below; keep the two branches equivalent).
  if (executable.wstring().find(L'\0') != std::wstring::npos) {
    if (error_out != nullptr) {
      *error_out = "executable path contains embedded NUL";
    }
    return -1;
  }
  for (const std::string& arg : args) {
    if (arg.find('\0') != std::string::npos) {
      if (error_out != nullptr) {
        *error_out = "argument contains embedded NUL";
      }
      return -1;
    }
  }
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

  std::vector<wchar_t> child_environment;
  if (!env_overrides.empty()) {
    const std::unique_ptr<wchar_t, decltype(&::FreeEnvironmentStringsW)> parent(
        ::GetEnvironmentStringsW(), ::FreeEnvironmentStringsW);
    if (!parent) {
      if (error_out != nullptr) {
        *error_out = "GetEnvironmentStringsW failed";
      }
      return -1;
    }
    const auto name_less = [](const std::wstring& a, const std::wstring& b) {
      return ::CompareStringOrdinal(a.c_str(), -1, b.c_str(), -1, TRUE) ==
             CSTR_LESS_THAN;
    };
    std::map<std::wstring, std::wstring, decltype(name_less)> variables(
        name_less);
    for (const wchar_t* entry = parent.get(); *entry != L'\0';) {
      const std::wstring_view pair(entry);
      // Windows includes special drive-directory entries such as =C:=C:\\.
      const size_t split = pair.find(L'=', pair.starts_with(L'=') ? 1 : 0);
      if (split != std::wstring_view::npos) {
        variables.emplace(std::wstring(pair.substr(0, split)),
                          std::wstring(pair.substr(split + 1)));
      }
      entry += pair.size() + 1;
    }
    const auto widen =
        [](std::string_view text) -> std::optional<std::wstring> {
      if (text.empty()) {
        return std::wstring{};
      }
      if (text.size() > std::numeric_limits<int>::max()) {
        return std::nullopt;
      }
      // Use the same ANSI code page as the previous SetEnvironmentVariableA.
      const auto length = static_cast<int>(text.size());
      const int size = ::MultiByteToWideChar(CP_ACP, MB_ERR_INVALID_CHARS,
                                             text.data(), length, nullptr, 0);
      if (size <= 0) {
        return std::nullopt;
      }
      std::wstring result(static_cast<size_t>(size), L'\0');
      if (::MultiByteToWideChar(CP_ACP, MB_ERR_INVALID_CHARS, text.data(),
                                length, result.data(), size) != size) {
        return std::nullopt;
      }
      return result;
    };
    for (const auto& [name, value] : env_overrides) {
      auto wide_name = widen(name);
      auto wide_value = widen(value);
      if (!wide_name || !wide_value) {
        if (error_out != nullptr) {
          *error_out = "cannot convert environment override to UTF-16";
        }
        return -1;
      }
      variables.insert_or_assign(std::move(*wide_name), std::move(*wide_value));
    }
    for (const auto& [name, value] : variables) {
      child_environment.insert(child_environment.end(), name.begin(),
                               name.end());
      child_environment.push_back(L'=');
      child_environment.insert(child_environment.end(), value.begin(),
                               value.end());
      child_environment.push_back(L'\0');
    }
    child_environment.push_back(L'\0');
  }

  STARTUPINFOW si{};
  si.cb = sizeof(si);
  // Match the POSIX path: the child inherits the parent's redirected
  // stdin/stdout/stderr instead of getting fresh console handles.
  si.dwFlags |= STARTF_USESTDHANDLES;
  si.hStdInput = ::GetStdHandle(STD_INPUT_HANDLE);
  si.hStdOutput = ::GetStdHandle(STD_OUTPUT_HANDLE);
  si.hStdError = ::GetStdHandle(STD_ERROR_HANDLE);
  PROCESS_INFORMATION pi{};
  const bool inherit_environment = child_environment.empty();
  const BOOL ok =
      ::CreateProcessW(executable.c_str(), cmd.data(), nullptr, nullptr, TRUE,
                       inherit_environment ? 0 : CREATE_UNICODE_ENVIRONMENT,
                       inherit_environment ? nullptr : child_environment.data(),
                       nullptr, &si, &pi);

  if (!ok) {
    if (error_out != nullptr) {
      *error_out = "CreateProcessW failed for " + executable.string();
    }
    return -1;
  }
  ::WaitForSingleObject(pi.hProcess, INFINITE);
  DWORD exit_code = 1;
  const BOOL got_exit_code = ::GetExitCodeProcess(pi.hProcess, &exit_code);
  ::CloseHandle(pi.hThread);
  ::CloseHandle(pi.hProcess);
  if (!got_exit_code) {
    if (error_out != nullptr) {
      *error_out = "GetExitCodeProcess failed (Windows error " +
                   std::to_string(::GetLastError()) + ")";
    }
    return -1;
  }
  // -1 is reserved for "spawn failed"; clamp to the POSIX 0..255 range so
  // ExitProcess(0xFFFFFFFF) does not masquerade as a spawn failure.
  return static_cast<int>(exit_code & 0xffU);
#else
  // Construct all storage in the parent. Calling setenv or allocating after
  // fork can deadlock on locks inherited from other application threads.
  std::string exe_str = executable.string();
  std::vector<std::string> owned = args;
  // T2621: the exec family terminates every string at its first NUL, so a
  // silent truncation would hand the child different arguments than the
  // caller intended (with exit code 0 on success).  The env_overrides loop
  // below has always rejected NULs; hold argv to the same contract.
  if (exe_str.find('\0') != std::string::npos) {
    if (error_out != nullptr) {
      *error_out = "executable path contains embedded NUL";
    }
    return -1;
  }
  for (const std::string& arg : owned) {
    if (arg.find('\0') != std::string::npos) {
      if (error_out != nullptr) {
        *error_out = "argument contains embedded NUL";
      }
      return -1;
    }
  }
  std::vector<char*> argv;
  argv.reserve(owned.size() + 2);
  argv.push_back(exe_str.data());
  for (std::string& arg : owned) {
    argv.push_back(arg.data());
  }
  argv.push_back(nullptr);

  std::vector<std::string> environment;
  for (char** entry = Environment(); entry != nullptr && *entry != nullptr;
       ++entry) {
    environment.emplace_back(*entry);
  }
  for (const auto& [key, value] : env_overrides) {
    const std::string prefix = key + "=";
    std::erase_if(environment, [&](const std::string& entry) {
      return entry.starts_with(prefix);
    });
    environment.push_back(prefix + value);
  }
  std::vector<char*> envp;
  envp.reserve(environment.size() + 1);
  for (std::string& entry : environment) {
    envp.push_back(entry.data());
  }
  envp.push_back(nullptr);
  pid_t child = 0;
#if defined(__APPLE__) && defined(POSIX_SPAWN_CLOEXEC_DEFAULT)
  const int spawned = [&] {
    posix_spawnattr_t attributes{};
    int rc = InitSpawnAttributes(&attributes);
    if (rc != 0) {
      return rc;
    }
    posix_spawn_file_actions_t actions{};
    rc = ::posix_spawn_file_actions_init(&actions);
    if (rc == 0) {
      // Explicitly preserve the standard streams that are open and inheritable.
      for (int fd : {STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO}) {
        const int flags = ::fcntl(fd, F_GETFD);
        if (flags >= 0 && (flags & FD_CLOEXEC) == 0) {
          rc = ::posix_spawn_file_actions_addinherit_np(&actions, fd);
          if (rc != 0) {
            break;
          }
        }
      }
      if (rc == 0) {
        rc = ::posix_spawn(&child, exe_str.c_str(), &actions, &attributes,
                           argv.data(), envp.data());
      }
      (void)::posix_spawn_file_actions_destroy(&actions);
    }
    (void)::posix_spawnattr_destroy(&attributes);
    return rc;
  }();
#else
  const int spawned = ::posix_spawn(&child, exe_str.c_str(), nullptr, nullptr,
                                    argv.data(), envp.data());
#endif
  if (spawned != 0) {
    if (error_out != nullptr) {
      *error_out = "posix_spawn failed: " + std::string(std::strerror(spawned));
    }
    return -1;
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
      // T2627: distinguish signal death (OOM killer, crashes, timeout
      // kills) from a normal nonzero exit; the raw status code alone left
      // operators guessing whether the child crashed or was killed.
      if (WIFSIGNALED(status)) {
        *error_out =
            "process terminated by signal " + std::to_string(WTERMSIG(status));
#ifdef WCOREDUMP
        if (WCOREDUMP(status)) {
          *error_out += " (core dumped)";
        }
#endif
      } else if (WIFSTOPPED(status)) {
        *error_out = "process unexpectedly stopped (signal " +
                     std::to_string(WSTOPSIG(status)) + ")";
      } else {
        *error_out = "process terminated abnormally";
      }
    }
    return -1;
  }
  return WEXITSTATUS(status);
#endif
}

int OpenWriteFailingDescriptor() {
#ifdef TINYLAMB_OS_WINDOWS
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
  const int open_errno = fd < 0 ? errno : 0;
  ::unlink(pattern.c_str());
  if (fd < 0) {
    errno = open_errno;
  }
  return fd;
#endif
}

uint64_t CurrentProcessRssBytes() noexcept {
#ifdef TINYLAMB_OS_WINDOWS
  PROCESS_MEMORY_COUNTERS pmc{};
  if (::GetProcessMemoryInfo(::GetCurrentProcess(), &pmc, sizeof(pmc))) {
    return static_cast<uint64_t>(pmc.WorkingSetSize);
  }
  return 0;
#elif defined(__APPLE__)
  mach_task_basic_info info{};
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (::task_info(::mach_task_self(), MACH_TASK_BASIC_INFO,
                  reinterpret_cast<task_info_t>(&info),
                  &count) == KERN_SUCCESS) {
    return static_cast<uint64_t>(info.resident_size);
  }
  return 0;
#elif defined(__linux__)
  const int fd = ::open("/proc/self/statm", O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return 0;
  }
  // Seven unsigned 64-bit fields can be ~140 digits plus separators; use a
  // generously sized buffer so a large-memory host cannot truncate the line.
  std::array<char, 512> buf{};
  const ssize_t n = ::read(fd, buf.data(), buf.size() - 1);
  ::close(fd);
  if (n <= 0) {
    return 0;
  }
  char* end = nullptr;
  // /proc/self/statm fields: size resident shared ... (in pages).
  (void)::strtoull(buf.data(), &end, 10);
  // strtoull leaves end == buf.data() when it consumed no digits. The buffer
  // is zero-filled but the line can end in '\n', so testing *end alone is not
  // enough to detect a malformed first field; without this check the second
  // strtoull would re-parse the same bad input and report a bogus 0 RSS.
  if (end == buf.data() || end == nullptr || *end == '\0') {
    return 0;
  }
  char* resident_end = nullptr;
  const unsigned long long resident_pages = ::strtoull(end, &resident_end, 10);
  if (resident_end == end) {
    return 0;
  }
  const long page_size = ::sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    return 0;
  }
  const auto pages = static_cast<uint64_t>(resident_pages);
  const auto page_bytes = static_cast<uint64_t>(page_size);
  if (pages != 0 && page_bytes > std::numeric_limits<uint64_t>::max() / pages) {
    return std::numeric_limits<uint64_t>::max();
  }
  return pages * page_bytes;
#else
  return 0;
#endif
}

uint64_t PeakProcessRssBytes() noexcept {
#ifdef TINYLAMB_OS_WINDOWS
  PROCESS_MEMORY_COUNTERS pmc{};
  if (::GetProcessMemoryInfo(::GetCurrentProcess(), &pmc, sizeof(pmc))) {
    return static_cast<uint64_t>(pmc.PeakWorkingSetSize);
  }
  return 0;
#elif defined(__APPLE__) || defined(__linux__)
  struct rusage ru{};
  if (::getrusage(RUSAGE_SELF, &ru) != 0) {
    return 0;
  }
#ifdef __APPLE__
  // macOS ru_maxrss is in bytes.
  return ru.ru_maxrss > 0 ? static_cast<uint64_t>(ru.ru_maxrss) : 0;
#else
  // Linux ru_maxrss is in KiB.
  return ru.ru_maxrss > 0 ? static_cast<uint64_t>(ru.ru_maxrss) * 1024ULL : 0;
#endif
#else
  return 0;
#endif
}

bool ReleaseFreeHeapToOs() noexcept {
#if defined(TINYLAMB_HAVE_MIMALLOC_COLLECT) || \
    (defined(__linux__) && defined(__GLIBC__))
  static std::atomic<int64_t> last_purge_ns{0};
  static std::atomic<bool> purge_in_flight{false};
  const int64_t now_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  int64_t prev = last_purge_ns.load(std::memory_order_relaxed);
  // Only one caller per ~20 ms pays for the purge; concurrent callers return
  // false.
  if (now_ns - prev < 20'000'000LL) {
    return false;
  }
  // T2622: claim the window with an in-flight flag instead of stamping the
  // timestamp up front.  A purge that outlives the window (huge heaps) used
  // to let the next caller's CAS succeed while the first purge was still
  // running, starting concurrent trims; the window is now measured from
  // COMPLETION of the previous purge.
  bool expected = false;
  if (!purge_in_flight.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel)) {
    return false;
  }
#ifdef TINYLAMB_HAVE_MIMALLOC_COLLECT
  mi_collect(/*force=*/true);
#endif
#if defined(__linux__) && defined(__GLIBC__)
  ::malloc_trim(0);
#endif
  // Stamp after the purge finished (T2622) so the next window starts at
  // completion, not at start.
  last_purge_ns.store(std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::steady_clock::now().time_since_epoch())
                          .count(),
                      std::memory_order_relaxed);
  purge_in_flight.store(false, std::memory_order_release);
  return true;
#else
  return false;
#endif
}

namespace {
// T2630: cgroup memory limit ("max"/absent/unparsable => 0 = unlimited).
// v1 encodes "no limit" as a huge PAGE_COUNTER_MAX sentinel, filtered by
// the >= 2^60 cutoff.
uint64_t ReadCgroupMemoryLimit(const char* path) noexcept {
  // noexcept boundary: iostream/string allocation failure must degrade to
  // "no limit" instead of terminating the process.
  try {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
      return 0;
    }
    std::string text;
    std::getline(in, text);
    while (!text.empty() &&
           (text.back() == '\r' || text.back() == '\n' || text.back() == ' ')) {
      text.pop_back();
    }
    if (text.empty() || text == "max") {
      return 0;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long long value = std::strtoull(text.c_str(), &end, 10);
    // Reject trailing garbage as well: query_grant.cpp's ReadGrantEnvU64 uses
    // the same strict "*end == '\0'" rule, so a value like "1073741824garbage"
    // must not silently become 1073741824 here.
    if (errno != 0 || end == text.c_str() || *end != '\0' ||
        value >= (1ULL << 60)) {
      return 0;
    }
    return static_cast<uint64_t>(value);
  } catch (...) {
    return 0;
  }
}
}  // namespace

uint64_t PhysicalMemoryBytes() noexcept {
#if defined(__linux__) || defined(__APPLE__)
  const long pages = ::sysconf(_SC_PHYS_PAGES);
  const long page_size = ::sysconf(_SC_PAGESIZE);
  uint64_t total = 0;
  if (pages > 0 && page_size > 0) {
    const auto page_count = static_cast<uint64_t>(pages);
    const auto bytes_per_page = static_cast<uint64_t>(page_size);
    constexpr auto max_bytes = std::numeric_limits<uint64_t>::max();
    total = page_count > max_bytes / bytes_per_page
                ? max_bytes
                : page_count * bytes_per_page;
  }
#ifdef __linux__
  // T2630: inside a container sysconf reports the HOST's RAM; auto-sized
  // budgets (memory_budget.cpp TINYLAMB_MEMORY_LIMIT_BYTES=auto) derived
  // from it overshot the cgroup and got the process OOM-killed.  Clamp by
  // the cgroup v2 / v1 memory limit when one is set.
  const uint64_t v2 = ReadCgroupMemoryLimit("/sys/fs/cgroup/memory.max");
  const uint64_t v1 =
      ReadCgroupMemoryLimit("/sys/fs/cgroup/memory/memory.limit_in_bytes");
  for (const uint64_t limit : {v2, v1}) {
    if (limit > 0 && (total == 0 || limit < total)) {
      total = limit;
    }
  }
#endif
  return total;
#else
  return 0;
#endif
}

}  // namespace tinylamb::platform
