/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "query/googlesql_frontend.hpp"

#include "common/platform/os.hpp"
#include "common/platform/process.hpp"
#include "common/platform/socket.hpp"

#ifdef TINYLAMB_OS_POSIX
#include <fcntl.h>
#include <poll.h>
// NOLINTNEXTLINE(modernize-deprecated-headers) POSIX sigset_t APIs
#include <signal.h>
#include <spawn.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#elif defined(TINYLAMB_OS_WINDOWS)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <mutex>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/unwind_guard.hpp"

namespace tinylamb {
namespace {

// Everything in this anonymous namespace is only reachable when the external
// GoogleSQL parser is wired in.
#if defined(TINYLAMB_GOOGLESQL_EXECUTABLE) && \
    (defined(TINYLAMB_OS_POSIX) || defined(TINYLAMB_OS_WINDOWS))

GoogleSqlParseResult RetryableParserFailure(std::string error) {
  return {.ok = false, .ast = {}, .error = std::move(error), .retryable = true};
}

// Child stdout and stderr share one pipe; a stderr warning can precede the
// parser's "ERROR:" line, so a leading-only check would misclassify an error
// as a successful AST. A successful dump, however, echoes literal contents
// verbatim: a multi-line literal can hold a line starting with "ERROR:".
// Treat only diagnostics before the AST dump begins as parse errors.
std::string_view ParserErrorLine(std::string_view output) {
  size_t line_start = 0;
  while (line_start < output.size()) {
    const size_t newline = output.find('\n', line_start);
    const size_t line_end =
        newline == std::string_view::npos ? output.size() : newline;
    std::string_view line = output.substr(line_start, line_end - line_start);
    const size_t first = line.find_first_not_of(" \t\r");
    line.remove_prefix(first == std::string_view::npos ? line.size() : first);
    if (line.starts_with("ERROR:")) {
      return line;
    } if (!line.empty() && !line.starts_with("WARNING:") &&
               !line.starts_with("INFO:")) {
      // The first non-diagnostic line is the root node of the AST dump.
      break;
    }
    if (newline == std::string_view::npos) {
      break;
    }
    line_start = newline + 1;
  }
  return {};
}

constexpr size_t kParseCacheShards = 16;
constexpr size_t kMaxCachedStatements = 1024;
constexpr size_t kMaxCachedStatementsPerShard =
    kMaxCachedStatements / kParseCacheShards;
// Bound cache memory as well as entry count: keys are the full SQL text, so
// a stream of near-maximum-size queries with unique suffixes can otherwise
// pin gigabytes while staying under the count limit.
constexpr size_t kMaxParseCacheKeyBytesPerShard = 4ULL << 20;
// Failed parses are remembered briefly so retries of the same broken SQL do
// not fork one parser process per attempt.
constexpr std::chrono::seconds kNegativeCacheTtl{1};
// Hard bound for one parse round trip; a stuck or flooded child is killed
// instead of blocking the worker forever.
constexpr std::chrono::seconds kParseTimeout{5};

size_t MaxCachedStatementsPerShard() {
  static const size_t kPerShard = []() -> size_t {
    if (const char* env = std::getenv("TINYLAMB_PARSE_CACHE_SIZE")) {
      if (env[0] == '-' || env[0] == '+') {
        return kMaxCachedStatementsPerShard;
      }
      char* end = nullptr;
      errno = 0;
      const unsigned long long val = std::strtoull(env, &end, 10);
      if (errno != 0 || end == env || *end != '\0') {
        return kMaxCachedStatementsPerShard;
      }
      const auto per_shard = val / kParseCacheShards;
      // Keep the default if the requested capacity cannot be represented.
      if (per_shard == 0 || !std::in_range<size_t>(per_shard)) {
        return kMaxCachedStatementsPerShard;
      }
      return static_cast<size_t>(per_shard);
    }
    return kMaxCachedStatementsPerShard;
  }();
  return kPerShard;
}

std::chrono::seconds NegativeCacheTtlSeconds() {
  static const std::chrono::seconds kTtl = []() {
    if (const char* env = std::getenv("TINYLAMB_NEGATIVE_CACHE_TTL_SEC")) {
      char* end = nullptr;
      errno = 0;
      const long val = std::strtol(env, &end, 10);
      if (errno == 0 && val > 0 && end != nullptr && end != env &&
          *end == '\0') {
        return std::chrono::seconds(val);
      }
    }
    return kNegativeCacheTtl;
  }();
  return kTtl;
}

// Tests that exercise EINTR retry on multi-MB inputs under sanitizer
// slowdown need a larger round-trip budget than the production default.
std::chrono::seconds ParseTimeoutSeconds() {
  if (const char* env = std::getenv("TINYLAMB_PARSE_TIMEOUT_SEC")) {
    char* end = nullptr;
    errno = 0;
    const long value = std::strtol(env, &end, 10);
    // Cap well below the point where adding seconds to a nanosecond
    // steady_clock time_point would overflow its signed rep.
    constexpr long kMaxParseTimeoutSec = 30LL * 24 * 3600;  // 30 days
    if (errno == 0 && value > 0 && value <= kMaxParseTimeoutSec && end != env &&
        *end == '\0') {
      return std::chrono::seconds(value);
    }
  }
  return kParseTimeout;
}

constexpr size_t kMaxChildOutputBytes = 64U << 20;

struct ParseShard {
  std::mutex mutex;
  std::condition_variable parse_finished;
  std::unordered_set<std::string_view> pending;
  std::unordered_map<std::string, std::string> cache;
  size_t cached_key_bytes{0};
  std::unordered_map<std::string, std::chrono::steady_clock::time_point>
      negative_cache;
  size_t negative_key_bytes{0};
};

std::array<ParseShard, kParseCacheShards> parse_shards;

// Bound child concurrency independently of the cache shard a statement hashes
// to. A slow parse leaves both cache hits and other keys free to proceed.
class ParseProcessSlots {
 public:
  void lock() {
    std::unique_lock lock(mutex_);
    ready_.wait(lock, [this] { return running_ < kParseCacheShards; });
    ++running_;
  }

  void unlock() {
    {
      std::scoped_lock lock(mutex_);
      --running_;
    }
    ready_.notify_one();
  }

 private:
  std::mutex mutex_;
  std::condition_variable ready_;
  size_t running_{0};
};

ParseProcessSlots parse_process_slots;

#ifdef TINYLAMB_OS_POSIX
// Give an unresponsive zygote time to recover while new requests use spawn.
constexpr std::chrono::seconds kZygoteRetryDelay{5};
std::atomic<std::chrono::steady_clock::time_point> zygote_retry_after{
    std::chrono::steady_clock::time_point::min()};
#endif

ParseShard& ShardFor(std::string_view sql) {
  return parse_shards[std::hash<std::string_view>{}(sql) % kParseCacheShards];
}

std::filesystem::path ResolveGoogleSqlExecutable() {
  if (const char* env = std::getenv("TINYLAMB_GOOGLESQL_BIN");
      env != nullptr && *env != '\0') {
    return {env};
  }
  std::filesystem::path configured(TINYLAMB_GOOGLESQL_EXECUTABLE);
  std::error_code ec;
  if (std::filesystem::exists(configured, ec)) {
    return configured;
  }
  const std::filesystem::path exe_dir =
      platform::CurrentExecutablePath().parent_path();
  if (!exe_dir.empty()) {
#ifdef TINYLAMB_OS_WINDOWS
    const std::filesystem::path sibling = exe_dir / "execute_query.exe";
#else
    const std::filesystem::path sibling = exe_dir / "execute_query";
#endif
    if (std::filesystem::exists(sibling, ec)) {
      return sibling;
    }
  }
  return configured;
}

#ifdef TINYLAMB_OS_WINDOWS

GoogleSqlParseResult ParseRawSubprocess(std::string_view sql) {
  const std::filesystem::path exe_path = ResolveGoogleSqlExecutable();
  SECURITY_ATTRIBUTES sa{};
  sa.nLength = sizeof(sa);
  sa.bInheritHandle = TRUE;
  sa.lpSecurityDescriptor = nullptr;

  HANDLE stdin_read = INVALID_HANDLE_VALUE;
  HANDLE stdin_write = INVALID_HANDLE_VALUE;
  if (!::CreatePipe(&stdin_read, &stdin_write, &sa, 0)) {
    return RetryableParserFailure("cannot create GoogleSQL pipe");
  }
  (void)::SetHandleInformation(stdin_write, HANDLE_FLAG_INHERIT, 0);

  HANDLE stdout_read = INVALID_HANDLE_VALUE;
  HANDLE stdout_write = INVALID_HANDLE_VALUE;
  if (!::CreatePipe(&stdout_read, &stdout_write, &sa, 0)) {
    ::CloseHandle(stdin_read);
    ::CloseHandle(stdin_write);
    return RetryableParserFailure("cannot create GoogleSQL pipe");
  }
  if (!::SetHandleInformation(stdout_read, HANDLE_FLAG_INHERIT, 0)) {
    ::CloseHandle(stdin_read);
    ::CloseHandle(stdin_write);
    ::CloseHandle(stdout_read);
    ::CloseHandle(stdout_write);
    return RetryableParserFailure(
        "cannot clear inherit flag on GoogleSQL stdout pipe");
  }

  STARTUPINFOW si{};
  si.cb = sizeof(si);
  si.dwFlags = STARTF_USESTDHANDLES;
  si.hStdInput = stdin_read;
  si.hStdOutput = stdout_write;
  si.hStdError = stdout_write;

  PROCESS_INFORMATION pi{};
  std::wstring cmdline = L"\"" + exe_path.wstring() + L"\" --mode=parse -";
  const BOOL started =
      ::CreateProcessW(exe_path.c_str(), cmdline.data(), nullptr, nullptr, TRUE,
                       CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi);
  ::CloseHandle(stdin_read);
  ::CloseHandle(stdout_write);
  if (!started) {
    ::CloseHandle(stdin_write);
    ::CloseHandle(stdout_read);
    return RetryableParserFailure("cannot start GoogleSQL");
  }

  std::string output;
  bool output_truncated = false;
  std::thread writer;
  std::thread reader;
  try {
    writer = std::thread([stdin_write, sql]() {
      size_t written = 0;
      while (written < sql.size()) {
        const DWORD chunk = static_cast<DWORD>(
            std::min<size_t>(sql.size() - written, 64U * 1024U));
        DWORD n = 0;
        if (!::WriteFile(stdin_write, sql.data() + written, chunk, &n,
                         nullptr)) {
          break;
        }
        written += n;
      }
      ::CloseHandle(stdin_write);
    });

    reader = std::thread([stdout_read, &output, &output_truncated]() {
      std::array<char, 4096> buffer{};
      for (;;) {
        DWORD n = 0;
        if (!::ReadFile(stdout_read, buffer.data(),
                        static_cast<DWORD>(buffer.size()), &n, nullptr) ||
            n == 0) {
          break;
        }
        output.append(buffer.data(), n);
        if (output.size() > kMaxChildOutputBytes) {
          output_truncated = true;
          break;
        }
      }
      ::CloseHandle(stdout_read);
    });

  } catch (...) {
    (void)::TerminateProcess(pi.hProcess, 1);
    (void)::WaitForSingleObject(pi.hProcess, INFINITE);
    if (writer.joinable()) {
      writer.join();
    } else {
      ::CloseHandle(stdin_write);
    }
    ::CloseHandle(stdout_read);
    ::CloseHandle(pi.hThread);
    ::CloseHandle(pi.hProcess);
    return RetryableParserFailure("cannot start parser pipe workers");
  }

  const auto timeout_ms =
      static_cast<DWORD>(std::chrono::duration_cast<std::chrono::milliseconds>(
                             ParseTimeoutSeconds())
                             .count());
  const DWORD wait_rc = ::WaitForSingleObject(pi.hProcess, timeout_ms);
  bool timed_out = false;
  if (wait_rc == WAIT_TIMEOUT) {
    timed_out = true;
    (void)::TerminateProcess(pi.hProcess, 1);
    (void)::WaitForSingleObject(pi.hProcess, INFINITE);
  }
  writer.join();
  reader.join();

  DWORD exit_code = 1;
  (void)::GetExitCodeProcess(pi.hProcess, &exit_code);
  ::CloseHandle(pi.hThread);
  ::CloseHandle(pi.hProcess);

  if (timed_out) {
    return RetryableParserFailure("GoogleSQL parser timed out");
  }
  if (output_truncated) {
    return {.ok = false,
            .ast = {},
            .error = "GoogleSQL parser output exceeds the size limit"};
  }
  if (exit_code != 0 || !ParserErrorLine(output).empty()) {
    return {.ok = false, .ast = {}, .error = std::move(output)};
  }
  // T1811: exit 0 with zero bytes of output was accepted unconditionally,
  // so a bogus TINYLAMB_GOOGLESQL_BIN (any silent exit-0 binary, a stub, a
  // wrong --mode) was indistinguishable from a successful empty parse: all
  // statements became "ok, empty AST" no-ops downstream.  Only whitespace
  // input legitimately parses to nothing; real SQL must produce an AST.
  if (output.empty() &&
      std::any_of(sql.begin(), sql.end(),
                  [](unsigned char c) { return std::isspace(c) == 0; })) {
    return {.ok = false,
            .ast = {},
            .error =
                "GoogleSQL parser exited 0 without output for non-empty "
                "SQL"};
  }
  return {.ok = true, .ast = std::move(output), .error = {}};
}

#else  // TINYLAMB_OS_POSIX

void CloseAll(std::array<int, 2>& pipes) {
  ::close(pipes[0]);
  ::close(pipes[1]);
}

// Writing to a pipe whose reader already died raises SIGPIPE, which would
// take down the whole worker; block it and rely on the EPIPE error instead.
class BlockedSigPipe {
 public:
  BlockedSigPipe() {
    sigemptyset(&set_);
    sigaddset(&set_, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &set_, &old_);
  }
  ~BlockedSigPipe() { pthread_sigmask(SIG_SETMASK, &old_, nullptr); }
  BlockedSigPipe(const BlockedSigPipe&) = delete;
  BlockedSigPipe& operator=(const BlockedSigPipe&) = delete;
  BlockedSigPipe(BlockedSigPipe&&) = delete;
  BlockedSigPipe& operator=(BlockedSigPipe&&) = delete;

 private:
  sigset_t set_{};
  sigset_t old_{};
};

// Lazily spawns a per-process persistent GoogleSQL parser daemon on first
// un-overridden parse when the configured binary supports `--zygote_sock=...`.
// Fork-safe (`owner_pid_ != getpid()` resets child tracking without reaping
// the parent's daemon) and gracefully falls back to per-query spawn if the
// configured binary is stock upstream `execute_query`.
class AutoZygoteManager {
 public:
  static AutoZygoteManager& Instance() {
    static AutoZygoteManager instance;
    return instance;
  }

  ~AutoZygoteManager() { Shutdown(/*permanent=*/true); }

  std::string EnsureSocketPath() {
    const auto retry_after = zygote_retry_after.load(std::memory_order_relaxed);
    if (retry_after != std::chrono::steady_clock::time_point::min() &&
        std::chrono::steady_clock::now() < retry_after) {
      return {};
    }
    if (const char* env_sock = std::getenv("TINYLAMB_GOOGLESQL_ZYGOTE_SOCK");
        env_sock != nullptr && *env_sock != '\0') {
      return {};
    }
    if (const char* disable = std::getenv("TINYLAMB_DISABLE_AUTO_ZYGOTE");
        disable != nullptr && std::strcmp(disable, "1") == 0) {
      return {};
    }
    if (const char* bin = std::getenv("TINYLAMB_GOOGLESQL_BIN");
        bin != nullptr && *bin != '\0') {
      return {};
    }
    std::scoped_lock lk(mu_);
    const pid_t self = ::getpid();
    if (owner_pid_ != 0 && owner_pid_ != self) {
      pid_ = -1;
      owner_pid_ = 0;
      sock_path_.clear();
      dir_path_.clear();
      disabled_ = false;
    }
    if (disabled_) {
      return {};
    }
    if (pid_ > 0 && !sock_path_.empty()) {
      return sock_path_;
    }

    const std::string exe = ResolveGoogleSqlExecutable().string();
    std::error_code ec;
    if (exe.empty() || !std::filesystem::exists(exe, ec)) {
      disabled_ = true;
      return {};
    }

    static std::atomic<uint64_t> seq{0};
    const uint64_t id = seq.fetch_add(1, std::memory_order_relaxed);
    std::string dir = "/tmp/tinylamb-gsql-" + std::to_string(::getuid()) + "-" +
                      std::to_string(self) + "-" + std::to_string(id);
    if (::mkdir(dir.c_str(), 0700) != 0) {
      disabled_ = true;
      return {};
    }
    std::string sock = dir + "/z.sock";
    struct sockaddr_un probe_addr{};
    if (sock.size() >= sizeof(probe_addr.sun_path)) {
      ::rmdir(dir.c_str());
      disabled_ = true;
      return {};
    }
    probe_addr.sun_family = AF_UNIX;
    std::strncpy(probe_addr.sun_path, sock.c_str(),
                 sizeof(probe_addr.sun_path) - 1);

    posix_spawn_file_actions_t actions{};
    int spawn_err = posix_spawn_file_actions_init(&actions);
    if (spawn_err == 0) {
      spawn_err = posix_spawn_file_actions_addopen(&actions, STDIN_FILENO,
                                                   "/dev/null", O_RDONLY, 0);
      if (spawn_err == 0) {
        spawn_err = posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO,
                                                     "/dev/null", O_WRONLY, 0);
      }
      if (spawn_err == 0) {
        spawn_err = posix_spawn_file_actions_addopen(&actions, STDERR_FILENO,
                                                     "/dev/null", O_WRONLY, 0);
      }
      pid_t child = -1;
      if (spawn_err == 0) {
        std::string arg0 = exe;
        std::string arg1 = "--zygote_sock=" + sock;
        std::array<char*, 3> argv = {arg0.data(), arg1.data(), nullptr};
        posix_spawnattr_t attributes{};
        spawn_err = platform::InitSpawnAttributes(&attributes);
        if (spawn_err == 0) {
          spawn_err = posix_spawn(&child, exe.c_str(), &actions, &attributes,
                                  argv.data(), platform::Environment());
          posix_spawnattr_destroy(&attributes);
        }
      }
      posix_spawn_file_actions_destroy(&actions);
      if (spawn_err != 0 || child <= 0) {
        ::rmdir(dir.c_str());
        disabled_ = true;
        return {};
      }

      for (int iter = 0; iter < 100; ++iter) {
        int st = 0;
        const pid_t w = ::waitpid(child, &st, WNOHANG);
        if (w == child || (w < 0 && errno != EINTR)) {
          ::unlink(sock.c_str());
          ::rmdir(dir.c_str());
          disabled_ = true;
          return {};
        }
        const int probe_fd = platform::OpenSocket(AF_UNIX, SOCK_STREAM, 0, {});
        if (probe_fd >= 0) {
          if (::connect(probe_fd,
                        reinterpret_cast<const struct sockaddr*>(&probe_addr),
                        sizeof(probe_addr)) == 0) {
            ::close(probe_fd);
            pid_ = child;
            owner_pid_ = self;
            sock_path_ = std::move(sock);
            dir_path_ = std::move(dir);
            static std::once_flag atexit_once;
            std::call_once(atexit_once, []() {
              std::atexit(
                  []() { AutoZygoteManager::Instance().Shutdown(true); });
            });
            return sock_path_;
          }
          ::close(probe_fd);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
      }

      ::kill(child, SIGTERM);
      bool reaped = false;
      for (int i = 0; i < 25; ++i) {
        int st = 0;
        const pid_t w = ::waitpid(child, &st, WNOHANG);
        if (w == child || (w < 0 && errno == ECHILD)) {
          reaped = true;
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
      }
      if (!reaped) {
        ::kill(child, SIGKILL);
        int st = 0;
        while (::waitpid(child, &st, 0) < 0 && errno == EINTR) {
        }
      }
      ::unlink(sock.c_str());
      ::rmdir(dir.c_str());
    }
    disabled_ = true;
    return {};
  }

  void Shutdown(bool permanent = true) {
    std::scoped_lock lk(mu_);
    if (permanent) {
      disabled_ = true;
    }
    if (owner_pid_ != ::getpid() || pid_ <= 0) {
      return;
    }
    const pid_t child = pid_;
    pid_ = -1;
    owner_pid_ = 0;
    ::kill(child, SIGTERM);
    bool reaped = false;
    for (int i = 0; i < 25; ++i) {
      int st = 0;
      const pid_t w = ::waitpid(child, &st, WNOHANG);
      if (w == child || (w < 0 && errno == ECHILD)) {
        reaped = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    if (!reaped) {
      ::kill(child, SIGKILL);
      int st = 0;
      while (::waitpid(child, &st, 0) < 0 && errno == EINTR) {
      }
    }
    if (!sock_path_.empty()) {
      ::unlink(sock_path_.c_str());
      sock_path_.clear();
    }
    if (!dir_path_.empty()) {
      ::rmdir(dir_path_.c_str());
      dir_path_.clear();
    }
  }

  void OnConnectFailure() {
    std::scoped_lock lk(mu_);
    if (owner_pid_ != ::getpid() || pid_ <= 0) {
      return;
    }
    int st = 0;
    const pid_t w = ::waitpid(pid_, &st, WNOHANG);
    if (w == pid_ || (w < 0 && errno == ECHILD)) {
      pid_ = -1;
      owner_pid_ = 0;
      disabled_ = false;
      zygote_retry_after.store(
          std::chrono::steady_clock::now() + kZygoteRetryDelay,
          std::memory_order_relaxed);
      if (!sock_path_.empty()) {
        ::unlink(sock_path_.c_str());
        sock_path_.clear();
      }
      if (!dir_path_.empty()) {
        ::rmdir(dir_path_.c_str());
        dir_path_.clear();
      }
    }
  }

 private:
  std::mutex mu_;
  pid_t pid_{-1};
  pid_t owner_pid_{0};
  std::string sock_path_;
  std::string dir_path_;
  bool disabled_{false};
};

// Optional Unix-domain socket fast-path for a pre-warmed GoogleSQL parser
// zygote (TINYLAMB_GOOGLESQL_ZYGOTE_SOCK, or AutoZygoteManager). Passes
// (stdin_fd, stdout_fd) via SCM_RIGHTS and returns the connected socket fd
// (which will yield a 1-byte exit status when the parse finishes), or -1 to
// fall back to posix_spawn.
int TryConnectZygote(int stdin_fd, int stdout_fd) {
  const auto retry_after = zygote_retry_after.load(std::memory_order_relaxed);
  if (retry_after != std::chrono::steady_clock::time_point::min() &&
      std::chrono::steady_clock::now() < retry_after) {
    return -1;
  }
  const char* sock_path = std::getenv("TINYLAMB_GOOGLESQL_ZYGOTE_SOCK");
  std::string auto_sock;
  bool using_auto = false;
  if (sock_path == nullptr || *sock_path == '\0') {
    auto_sock = AutoZygoteManager::Instance().EnsureSocketPath();
    if (auto_sock.empty()) {
      return -1;
    }
    sock_path = auto_sock.c_str();
    using_auto = true;
  }
  const int sock = platform::OpenSocket(AF_UNIX, SOCK_STREAM, 0, {});
  if (sock < 0) {
    return -1;
  }
  struct sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  if (std::strlen(sock_path) >= sizeof(addr.sun_path)) {
    ::close(sock);
    return -1;
  }
  std::strncpy(addr.sun_path, sock_path, sizeof(addr.sun_path) - 1);
  if (::connect(sock, reinterpret_cast<const struct sockaddr*>(&addr),
                sizeof(addr)) != 0) {
    ::close(sock);
    if (using_auto) {
      AutoZygoteManager::Instance().OnConnectFailure();
    }
    return -1;
  }
  std::array<int, 2> pass_fds = {stdin_fd, stdout_fd};
  char payload = 'x';
  struct iovec iov{};
  iov.iov_base = &payload;
  iov.iov_len = 1;
  alignas(struct cmsghdr) std::array<char, CMSG_SPACE(sizeof(pass_fds))>
      cmsg_buf{};
  struct msghdr msg{};
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = cmsg_buf.data();
  msg.msg_controllen = cmsg_buf.size();
  struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(pass_fds));
  std::memcpy(CMSG_DATA(cmsg), pass_fds.data(), sizeof(pass_fds));
  ssize_t sent = -1;
  do {
    sent = platform::SendMsgNoSignal(sock, &msg);
  } while (sent < 0 && errno == EINTR);
  if (sent <= 0) {
    ::close(sock);
    if (using_auto) {
      // The zygote accepted the connection but failed the fd hand-off; it is
      // just as broken as a refused connect and must leave the pool, or every
      // later parse waits on the same dead socket.
      AutoZygoteManager::Instance().OnConnectFailure();
    }
    return -1;
  }
  return sock;
}

GoogleSqlParseResult ParseRawSubprocess(
    std::string_view sql, bool allow_zygote = true,
    std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + ParseTimeoutSeconds()) {
  if (std::chrono::steady_clock::now() >= deadline) {
    return RetryableParserFailure("GoogleSQL parser timed out");
  }
  std::array<int, 2> input_pipe{};
  std::array<int, 2> output_pipe{};
  // Close-on-exec (plus POSIX_SPAWN_CLOEXEC_DEFAULT where the platform cannot
  // set it atomically) keeps concurrent parses' descriptors out of the child;
  // only the dup2'd std{in,out,err} survive exec.
  if (platform::OpenPipe(input_pipe, {}) != 0) {
    return RetryableParserFailure(
        std::string("cannot create GoogleSQL pipe: ") + std::strerror(errno));
  }
  if (platform::OpenPipe(output_pipe, {}) != 0) {
    const std::string reason(errno == EMFILE || errno == ENFILE
                                 ? std::string("too many open files")
                                 : std::strerror(errno));
    CloseAll(input_pipe);
    return RetryableParserFailure("cannot create GoogleSQL pipe: " + reason);
  }

  BlockedSigPipe blocked_sigpipe;
  const int zygote_sock =
      allow_zygote ? TryConnectZygote(input_pipe[0], output_pipe[1]) : -1;
  pid_t child = -1;
  if (zygote_sock < 0) {
    if (input_pipe[0] > STDERR_FILENO && output_pipe[1] > STDERR_FILENO) {
      posix_spawn_file_actions_t actions{};
      int spawn_err = posix_spawn_file_actions_init(&actions);
      if (spawn_err == 0) {
        spawn_err = posix_spawn_file_actions_adddup2(&actions, input_pipe[0],
                                                     STDIN_FILENO);
        if (spawn_err == 0) {
          spawn_err = posix_spawn_file_actions_adddup2(&actions, output_pipe[1],
                                                       STDOUT_FILENO);
        }
        if (spawn_err == 0) {
          spawn_err = posix_spawn_file_actions_adddup2(&actions, output_pipe[1],
                                                       STDERR_FILENO);
        }
        if (spawn_err == 0) {
          std::string arg0 = ResolveGoogleSqlExecutable().string();
          std::string arg1("--mode=parse");
          std::string arg2("-");
          std::array<char*, 4> argv = {arg0.data(), arg1.data(), arg2.data(),
                                       nullptr};
          posix_spawnattr_t attributes{};
          spawn_err = platform::InitSpawnAttributes(&attributes);
          if (spawn_err == 0) {
            spawn_err = posix_spawn(&child, arg0.c_str(), &actions, &attributes,
                                    argv.data(), platform::Environment());
            posix_spawnattr_destroy(&attributes);
          }
        }
        posix_spawn_file_actions_destroy(&actions);
      }
      if (spawn_err != 0) {
        const std::string reason(std::strerror(spawn_err));
        CloseAll(input_pipe);
        CloseAll(output_pipe);
        return RetryableParserFailure("cannot start GoogleSQL: " + reason);
      }
    } else {
      // Resolve in the parent: a fork()ed child of a multi-threaded process
      // may only run async-signal-safe code. std::getenv / std::filesystem /
      // readlink / std::string allocation can deadlock on a lock some other
      // thread held at fork time (see common/platform/process.cpp).
      const std::string resolved = ResolveGoogleSqlExecutable().string();
      child = fork();
      if (child < 0) {
        const std::string reason(std::strerror(errno));
        CloseAll(input_pipe);
        CloseAll(output_pipe);
        return RetryableParserFailure("cannot start GoogleSQL: " + reason);
      }
      if (child == 0) {
        dup2(input_pipe[0], STDIN_FILENO);
        dup2(output_pipe[1], STDOUT_FILENO);
        dup2(output_pipe[1], STDERR_FILENO);
        CloseAll(input_pipe);
        CloseAll(output_pipe);
        execl(resolved.c_str(), resolved.c_str(), "--mode=parse", "-",
              static_cast<char*>(nullptr));
        _exit(127);
      }
    }
  }

  close(input_pipe[0]);
  input_pipe[0] = -1;
  close(output_pipe[1]);
  output_pipe[1] = -1;

  bool input_open = true;
  bool output_open = true;
  size_t written = 0;
  std::string output;
  bool output_truncated = false;
  // Interleave writes and reads via poll: the child emits its AST while
  // still consuming the query, so a large result cannot deadlock a parent
  // that is stuck in write(). One syscall per readiness event keeps
  // the blocking descriptors from stalling the loop.
  while (input_open || output_open) {
    const auto remaining = deadline - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::steady_clock::duration::zero()) {
      break;
    }
    // Fully written (or empty) query: hand the child its EOF even when no
    // POLLOUT event will ever fire.
    if (input_open && written >= sql.size()) {
      close(input_pipe[1]);
      input_pipe[1] = -1;
      input_open = false;
    }
    const int timeout_ms = static_cast<int>(
        std::min<std::chrono::milliseconds>(
            std::chrono::duration_cast<std::chrono::milliseconds>(remaining),
            std::chrono::milliseconds(100))
            .count());
    // Pass fd = -1 for closed endpoints so poll() ignores them instead of
    // returning immediately with POLLNVAL and busy-spinning in kernel space.
    std::array<pollfd, 2> fds{};
    fds[0] = {.fd = output_open ? output_pipe[0] : -1,
              .events = static_cast<short>(output_open ? POLLIN : 0),
              .revents = 0};
    fds[1] = {.fd = (input_open && written < sql.size()) ? input_pipe[1] : -1,
              .events = static_cast<short>(
                  input_open && written < sql.size() ? POLLOUT : 0),
              .revents = 0};
    const int ready = ::poll(fds.data(), 2, timeout_ms);
    if (ready < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    if (ready == 0) {
      continue;
    }

    if (output_open &&
        (fds[0].revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL)) != 0) {
      if ((fds[0].revents & POLLNVAL) != 0) {
        output_pipe[0] = -1;
        output_open = false;
      } else {
        std::array<char, 4096> buffer{};
        const ssize_t count =
            read(output_pipe[0], buffer.data(), buffer.size());
        if (count > 0) {
          output.append(buffer.data(), static_cast<size_t>(count));
          if (output.size() > kMaxChildOutputBytes) {
            // A partial dump must never be reported as a successful parse
            // (nor cached): the AST reader would accept a truncated token.
            output_truncated = true;
            close(output_pipe[0]);
            output_pipe[0] = -1;
            output_open = false;
          }
        } else if (count == 0 || (errno != EINTR && errno != EAGAIN &&
                                  errno != EWOULDBLOCK)) {
          close(output_pipe[0]);
          output_pipe[0] = -1;
          output_open = false;
        }
      }
    }

    if (input_open && written < sql.size() &&
        (fds[1].revents & (POLLOUT | POLLHUP | POLLERR | POLLNVAL)) != 0) {
      if ((fds[1].revents & POLLNVAL) != 0) {
        input_pipe[1] = -1;
        input_open = false;
      } else {
        const ssize_t count =
            write(input_pipe[1], sql.data() + written, sql.size() - written);
        if (count >= 0) {
          written += static_cast<size_t>(count);
        } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
          // EPIPE: the child died early; stop feeding it.
          close(input_pipe[1]);
          input_pipe[1] = -1;
          input_open = false;
        }
      }
    }
  }
  if (output_open && output_pipe[0] >= 0) {
    close(output_pipe[0]);
  }
  if (input_open && input_pipe[1] >= 0) {
    close(input_pipe[1]);
  }

  if (zygote_sock >= 0) {
    uint8_t exit_code = 1;
    bool got_status = false;
    while (std::chrono::steady_clock::now() < deadline) {
      const auto remaining = deadline - std::chrono::steady_clock::now();
      const int timeout_ms = std::max(
          1, static_cast<int>(
                 std::min<std::chrono::milliseconds>(
                     std::chrono::duration_cast<std::chrono::milliseconds>(
                         remaining),
                     std::chrono::milliseconds(100))
                     .count()));
      pollfd pfd{.fd = zygote_sock, .events = POLLIN, .revents = 0};
      const int r = ::poll(&pfd, 1, timeout_ms);
      if (r < 0) {
        if (errno == EINTR) {
          continue;
        }
        break;
      }
      if (r == 0) {
        continue;
      }
      // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection)
      const ssize_t n = ::read(zygote_sock, &exit_code, 1);
      if (n == 1) {
        got_status = true;
        break;
      }
      if (n < 0 && errno == EINTR) {
        continue;
      }
      break;
    }
    ::close(zygote_sock);
    if (!got_status) {
      // Retry this parse via spawn and let subsequent requests reconnect
      // once the cooldown expires. The recursive call skips the zygote.
      zygote_retry_after.store(
          std::chrono::steady_clock::now() + kZygoteRetryDelay,
          std::memory_order_relaxed);
      AutoZygoteManager::Instance().Shutdown(/*permanent=*/false);
      // The zygote consumed the original deadline while hanging. Passing it
      // on would make the spawn branch fail its first "already expired"
      // check, so the documented fallback never actually ran. Give spawn a
      // fresh budget; allow_zygote=false keeps this a one-shot retry.
      return ParseRawSubprocess(
          sql, /*allow_zygote=*/false,
          std::chrono::steady_clock::now() + ParseTimeoutSeconds());
    }
    if (output_truncated) {
      GoogleSqlParseResult truncated;
      truncated.ok = false;
      truncated.error = "GoogleSQL parser output exceeds the size limit";
      return truncated;
    }
    if (exit_code != 0 || !ParserErrorLine(output).empty()) {
      return {.ok = false, .ast = {}, .error = std::move(output)};
    }
    if (output.empty() &&
        std::any_of(sql.begin(), sql.end(),
                    [](unsigned char c) { return std::isspace(c) == 0; })) {
      return {.ok = false,
              .ast = {},
              .error =
                  "GoogleSQL parser exited 0 without output for non-empty SQL"};
    }
    return {.ok = true, .ast = std::move(output), .error = {}};
  }

  // Reap the child with a deadline; escalate to SIGKILL on overrun.
  int status = 0;
  bool reaped = false;
  std::chrono::microseconds poll_backoff{50};
  while (std::chrono::steady_clock::now() < deadline) {
    const pid_t done = waitpid(
        child, &status, WNOHANG);  // NOLINT(misc-include-cleaner) glibc macro
    if (done == child) {
      reaped = true;
      break;
    }
    if (done < 0 && errno == EINTR) {
      continue;
    }
    std::this_thread::sleep_for(poll_backoff);
    poll_backoff = std::min(poll_backoff * 2, std::chrono::microseconds(2000));
  }
  if (!reaped) {
    kill(child, SIGKILL);
    while (waitpid(child, &status, 0) < 0 && errno == EINTR) {
    }
    return RetryableParserFailure("GoogleSQL parser timed out");
  }
  if (!WIFEXITED(status) ||        // NOLINT(misc-include-cleaner) glibc macro
      WEXITSTATUS(status) != 0) {  // NOLINT(misc-include-cleaner) glibc macro
    return {.ok = false, .ast = {}, .error = std::move(output)};
  }
  if (output_truncated) {
    GoogleSqlParseResult truncated;
    truncated.ok = false;
    truncated.error = "GoogleSQL parser output exceeds the size limit";
    return truncated;
  }
  if (!ParserErrorLine(output).empty()) {
    return {.ok = false, .ast = {}, .error = std::move(output)};
  }
  // T1811: same guard as the Windows implementation above -- exit 0 with no
  // output is only a valid "empty SQL" result for whitespace input; a broken
  // binary that says nothing and succeeds must not masquerade as an empty AST.
  if (output.empty() &&
      std::any_of(sql.begin(), sql.end(),
                  [](unsigned char c) { return std::isspace(c) == 0; })) {
    return {.ok = false,
            .ast = {},
            .error =
                "GoogleSQL parser exited 0 without output for non-empty "
                "SQL"};
  }
  return {.ok = true, .ast = std::move(output), .error = {}};
}

#endif  // TINYLAMB_OS_WINDOWS

std::string ToLowerCopy(std::string_view s) {
  std::string result;
  result.reserve(s.size());
  for (char c : s) {
    result.push_back(
        static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return result;
}

// Returns a same-length copy of `sql` with every byte inside a string
// literal (single/double/triple-quoted, including raw/bytes variants), a
// backtick identifier, or a comment replaced by spaces.  Keyword searches on
// the mask therefore cannot match inside literals, identifiers or comments;
// positions found in the mask index the original text identically.
// Pre-rewrites that ignored this masking corrupted queries such as
// SELECT 'with ties' (the rewrite deleted bytes from inside the literal).
std::string MaskLiteralsAndComments(std::string_view sql) {
  std::string masked(sql);
  const auto blank = [&masked](size_t begin, size_t end) {
    for (size_t k = begin; k < end && k < masked.size(); ++k) {
      masked[k] = ' ';
    }
  };
  size_t i = 0;
  while (i < sql.size()) {
    const char c = sql[i];
    if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
      const size_t begin = i;
      while (i < sql.size() && sql[i] != '\n') {
        ++i;
      }
      blank(begin, i);
      continue;
    }
    if (c == '#') {
      const size_t begin = i;
      while (i < sql.size() && sql[i] != '\n') {
        ++i;
      }
      blank(begin, i);
      continue;
    }
    if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
      const size_t begin = i;
      i += 2;
      while (i + 1 < sql.size() && (sql[i] != '*' || sql[i + 1] != '/')) {
        ++i;
      }
      i = std::min(sql.size(), i + 2);
      blank(begin, i);
      continue;
    }
    if ((c == '\'' || c == '"') && i + 2 < sql.size() && sql[i + 1] == c &&
        sql[i + 2] == c) {
      const size_t begin = i;
      const char quote = c;
      i += 3;
      while (i < sql.size()) {
        if (sql[i] == quote && i + 2 < sql.size() && sql[i + 1] == quote &&
            sql[i + 2] == quote) {
          i += 3;
          break;
        }
        if (sql[i] == '\\' && i + 1 < sql.size()) {
          ++i;
        }
        ++i;
      }
      blank(begin, std::min(i, sql.size()));
      continue;
    }
    if (c == '\'' || c == '"') {
      const size_t begin = i;
      const char quote = c;
      ++i;
      while (i < sql.size() && sql[i] != quote) {
        if (sql[i] == '\\' && i + 1 < sql.size()) {
          i += 2;
          continue;
        }
        ++i;
      }
      i = std::min(sql.size(), i + 1);
      blank(begin, i);
      continue;
    }
    if (c == '$') {
      // Dollar-quoted string: $tag$ ... $tag$ with an optional identifier
      // tag.  GoogleSQL literals can legitimately contain quotes and
      // keyword-bearing text, so they must be masked like the quoted forms.
      const size_t begin = i;
      size_t j = i + 1;
      while (j < sql.size() &&
             (std::isalnum(static_cast<unsigned char>(sql[j])) != 0 ||
              sql[j] == '_')) {
        ++j;
      }
      if (j < sql.size() && sql[j] == '$') {
        const std::string tag(sql.substr(i, j - i + 1));
        const size_t end = sql.find(tag, j + 1);
        i = end == std::string_view::npos ? sql.size() : end + tag.size();
        blank(begin, std::min(i, sql.size()));
        continue;
      }
      // Not a dollar quote (parameter placeholder or similar); leave the
      // byte in place.
      ++i;
      continue;
    }
    if (c == '`') {
      const size_t begin = i;
      ++i;
      while (i < sql.size() && sql[i] != '`') {
        ++i;
      }
      i = std::min(sql.size(), i + 1);
      blank(begin, i);
      continue;
    }
    ++i;
  }
  return masked;
}

bool ExtractDistinctOn(std::string_view sql, size_t* out_start, size_t* out_end,
                       std::string* out_exprs) {
  std::string lower = ToLowerCopy(sql);
  // Search a literal/comment-masked copy: "distinct on (" inside a string
  // literal, backtick identifier or comment must never trigger the rewrite.
  const std::string mask = ToLowerCopy(MaskLiteralsAndComments(sql));
  size_t pos = 0;
  while (pos < lower.size()) {
    size_t found = mask.find("distinct", pos);
    if (found == std::string::npos) {
      break;
    }
    // The boundary check must read the masked copy: a literal that ends just
    // before "distinct" is blanked there, while the raw text still has an
    // alphanumeric quote-adjacent byte and would reject a valid rewrite.
    if (found > 0 &&
        ((std::isalnum(static_cast<unsigned char>(mask[found - 1])) != 0) ||
         mask[found - 1] == '_' || mask[found - 1] == '$')) {
      pos = found + 8;
      continue;
    }
    // Symmetric trailing boundary: "distinctx" is an identifier, not the
    // DISTINCT keyword (FindOuterKeyword checks both sides already).
    if (found + 8 < mask.size() &&
        ((std::isalnum(static_cast<unsigned char>(mask[found + 8])) != 0) ||
         mask[found + 8] == '_' || mask[found + 8] == '$')) {
      pos = found + 8;
      continue;
    }
    // Scan tokens on the masked copy: a comment between DISTINCT and ON (or
    // ON and '(') is replaced by spaces there, while raw `lower` still has
    // '/' and '*' and would make the rewrite silently no-op.
    size_t after_dist = found + 8;
    while (after_dist < mask.size() &&
           (std::isspace(static_cast<unsigned char>(mask[after_dist])) != 0)) {
      ++after_dist;
    }
    if (mask.compare(after_dist, 2, "on") == 0) {
      size_t after_on = after_dist + 2;
      if (after_on < mask.size() &&
          ((std::isalnum(static_cast<unsigned char>(mask[after_on])) != 0) ||
           mask[after_on] == '_' || mask[after_on] == '$')) {
        pos = after_on;
        continue;
      }
      while (after_on < mask.size() &&
             (std::isspace(static_cast<unsigned char>(mask[after_on])) != 0)) {
        ++after_on;
      }
      if (after_on < mask.size() && mask[after_on] == '(') {
        // Depth-count on the literal-masked copy: mask and sql share
        // positions 1:1, and counting on raw text would let an unbalanced
        // parenthesis inside a string literal (e.g. ON ('a)')) corrupt the
        // slice.
        size_t open_paren = after_on;
        int depth = 1;
        size_t close_paren = open_paren + 1;
        while (close_paren < mask.size() && depth > 0) {
          if (mask[close_paren] == '(') {
            ++depth;
          } else if (mask[close_paren] == ')') {
            --depth;
          }
          ++close_paren;
        }
        if (depth == 0) {
          *out_start = found;
          *out_end = close_paren;
          *out_exprs = std::string(
              sql.substr(open_paren + 1, close_paren - 1 - open_paren - 1));
          return true;
        }
      }
    }
    pos = found + 8;
  }
  return false;
}

// Prepasses may only rewrite a whole keyword at the outer query level.
// Literal/comment bytes have already been blanked by the caller.
size_t FindOuterKeyword(std::string_view mask, std::string_view keyword,
                        size_t from = 0) {
  const auto identifier = [](char c) {
    return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_';
  };
  size_t depth = 0;
  for (size_t i = 0; i < mask.size(); ++i) {
    if (mask[i] == '(') {
      ++depth;
      continue;
    }
    if (mask[i] == ')') {
      if (depth > 0) {
        --depth;
      }
      continue;
    }
    if (depth != 0 || i < from || (i != 0 && identifier(mask[i - 1]))) {
      continue;
    }
    if (mask.substr(i).starts_with(keyword) &&
        (i + keyword.size() == mask.size() ||
         !identifier(mask[i + keyword.size()]))) {
      return i;
    }
  }
  return std::string_view::npos;
}

bool ExtractFetchWithTies(std::string_view sql, std::string* out_rewritten,
                          bool* out_with_ties) {
  // Keyword positions come from the literal/comment mask so that a "with
  // ties" / "fetch first" inside a string literal or comment is ignored.
  const std::string mask = ToLowerCopy(MaskLiteralsAndComments(sql));
  std::string lower = ToLowerCopy(sql);
  // The numeric argument scan below runs over the original text; skip block
  // and line comments together with whitespace so a comment between the
  // keyword and the number does not collapse LIMIT to its "1" fallback.
  const auto skip_space_and_comments = [&](size_t cur, size_t end) {
    while (cur < end) {
      if (std::isspace(static_cast<unsigned char>(lower[cur])) != 0) {
        ++cur;
      } else if (lower.compare(cur, 2, "/*") == 0) {
        const size_t close = lower.find("*/", cur + 2);
        cur = close == std::string::npos ? end : close + 2;
      } else if (lower.compare(cur, 2, "--") == 0) {
        const size_t eol = lower.find('\n', cur + 2);
        cur = eol == std::string::npos ? end : eol + 1;
      } else {
        break;
      }
    }
    return cur;
  };
  *out_with_ties = false;
  const size_t limit_pos = FindOuterKeyword(mask, "limit");
  size_t fetch_pos = FindOuterKeyword(mask, "fetch first");
  if (fetch_pos == std::string::npos) {
    fetch_pos = FindOuterKeyword(mask, "fetch next");
  }

  // WITH ties AS (...) names a CTE, not a row-limit modifier. A ties
  // marker must follow the clause whose behavior it changes.
  const size_t bound_pos =
      fetch_pos != std::string::npos ? fetch_pos : limit_pos;
  const size_t ties_pos = bound_pos == std::string::npos
                              ? std::string::npos
                              : FindOuterKeyword(mask, "with ties", bound_pos);
  const bool with_ties = ties_pos != std::string::npos;
  *out_with_ties = with_ties;

  if (fetch_pos != std::string::npos) {
    // LIMIT/OFFSET operands are constant expressions and the engine folds
    // them (OFFSET 5-1 == 4, FETCH FIRST 2+1 == 3). Keeping only leading
    // digits silently dropped the tail and returned the wrong rows.
    const auto is_ident = [](char c) {
      return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_' ||
             c == '$';
    };
    // ROW/ROWS terminates the argument. Search the masked text so a keyword
    // inside a literal or comment cannot end the expression early.
    const auto argument_end = [&](size_t from, size_t end) {
      size_t cur = from;
      while (cur < end && lower[cur] != ';') {
        const bool word_start = cur == 0 || !is_ident(mask[cur - 1]);
        if (word_start &&
            ((mask.compare(cur, 4, "rows") == 0 &&
              (cur + 4 >= mask.size() || !is_ident(mask[cur + 4]))) ||
             (mask.compare(cur, 3, "row") == 0 &&
              (cur + 3 >= mask.size() || !is_ident(mask[cur + 3]))))) {
          break;
        }
        ++cur;
      }
      return cur;
    };
    const auto argument = [&](size_t from, size_t end) {
      while (from < end &&
             std::isspace(static_cast<unsigned char>(lower[from])) != 0) {
        ++from;
      }
      while (end > from &&
             std::isspace(static_cast<unsigned char>(lower[end - 1])) != 0) {
        --end;
      }
      return std::string(sql.substr(from, end - from));
    };

    size_t offset_pos = FindOuterKeyword(mask, "offset");
    std::string offset_val;
    if (offset_pos != std::string::npos && offset_pos < fetch_pos) {
      const size_t cur = skip_space_and_comments(offset_pos + 6, fetch_pos);
      offset_val = argument(cur, argument_end(cur, fetch_pos));
    }

    const size_t count_start =
        skip_space_and_comments(fetch_pos + 11, lower.size());
    std::string count_val =
        argument(count_start, argument_end(count_start, lower.size()));
    if (count_val.empty()) {
      count_val = "1";
    }

    size_t cut_pos = (offset_pos != std::string::npos && offset_pos < fetch_pos)
                         ? offset_pos
                         : fetch_pos;
    std::string base = std::string(sql.substr(0, cut_pos));
    base += "LIMIT " + count_val;
    if (!offset_val.empty()) {
      base += " OFFSET " + offset_val;
    }
    if (sql.ends_with(';')) {
      base += ";";
    }
    *out_rewritten = base;
    return true;
  }

  if (with_ties) {
    std::string modified = std::string(sql);
    size_t pos = ties_pos;
    if (pos != std::string::npos) {
      modified.erase(pos, 9);
      *out_rewritten = modified;
      return true;
    }
  }

  return false;
}

std::string NormalizeGroupByDistinct(std::string_view sql) {
  std::string lower = ToLowerCopy(sql);
  // The mask keeps a "group by distinct" inside a literal/comment from
  // being rewritten into invalid SQL.  Comments are blanked to spaces, so
  // the keyword sequence is matched with arbitrary whitespace between the
  // words — a literal find() misses `GROUP /*c*/ BY DISTINCT`.
  const std::string mask = ToLowerCopy(MaskLiteralsAndComments(sql));
  auto skip_ws = [&](size_t i) {
    while (i < mask.size() &&
           std::isspace(static_cast<unsigned char>(mask[i])) != 0) {
      ++i;
    }
    return i;
  };
  auto ident_char = [](char c) {
    return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_' ||
           c == '$';
  };
  for (size_t pos = mask.find("group"); pos != std::string::npos;
       pos = mask.find("group", pos + 1)) {
    if (pos > 0 && ident_char(mask[pos - 1])) {
      continue;  // "mygroup": not the GROUP keyword.
    }
    size_t p = skip_ws(pos + 5);
    if (p + 2 > mask.size() || mask.compare(p, 2, "by") != 0 ||
        (p + 2 < mask.size() && ident_char(mask[p + 2]))) {
      continue;
    }
    p = skip_ws(p + 2);
    if (p + 8 > mask.size() || mask.compare(p, 8, "distinct") != 0) {
      continue;
    }
    const size_t distinct_end = p + 8;
    // `distinct_1` is an ordinary identifier, not the GROUP BY DISTINCT
    // modifier.  Rewriting it would remove the identifier prefix and turn
    // the query into the invalid token `GROUP BY_1`.
    if (distinct_end < lower.size() && ident_char(lower[distinct_end])) {
      continue;
    }
    size_t after = distinct_end;
    while (after < lower.size() &&
           (std::isspace(static_cast<unsigned char>(lower[after])) != 0)) {
      ++after;
    }
    // `all` must be a whole word: `GROUP BY DISTINCT allocate` names a column,
    // and matching its "all" prefix would rewrite it into `GROUP BY ALLOCATE`.
    if (lower.compare(after, 3, "all") == 0 &&
        (after + 3 >= lower.size() || !ident_char(lower[after + 3]))) {
      std::string result(sql.substr(0, pos));
      result += "GROUP BY ALL";
      result += sql.substr(after + 3);
      return result;
    }
    std::string result(sql.substr(0, pos));
    result += "GROUP BY";
    result += sql.substr(distinct_end);
    return result;
  }
  return std::string(sql);
}

namespace {

// The pinned parser has no ILIKE token, so `x [NOT] ILIKE y` is rewritten to
// the equivalent `LOWER(x) [NOT] LIKE LOWER(y)` before parsing. Every extent
// check is conservative: when an operand boundary cannot be proven the
// rewrite bails and the statement fails to parse loudly — never rewritten
// into different semantics.
bool IsIdentChar(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_' ||
         c == '$';
}

bool IsSqlSpace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

// Keywords that may legally sit immediately before a comparison operand.
// An identifier-shaped atom whose predecessor is a different identifier (or
// nothing on this list) means the "atom" is actually part of a larger
// construction the rewrite does not understand.
bool BoundaryKeywordBeforeOperand(std::string_view word) {
  static constexpr std::array<std::string_view, 16> kKeywords = {
      "SELECT", "WHERE", "AND",     "OR",   "NOT",  "HAVING",   "BY",  "WHEN",
      "THEN",   "ON",    "QUALIFY", "CASE", "ELSE", "DISTINCT", "ALL", "ANY"};
  return std::ranges::any_of(
      kKeywords, [word](std::string_view keyword) { return word == keyword; });
}

// Scans backward from `pos` (exclusive) over the atom ending there and
// returns the atom start, or std::string::npos when the text before `pos`
// is not an operand this rewrite understands. Literal interiors are
// detected through the position-preserving mask (blanked).
size_t LikeOperandBegin(std::string_view sql, std::string_view mask,
                        size_t pos) {
  size_t j = pos;
  while (j > 0 && IsSqlSpace(sql[j - 1])) {
    --j;
  }
  if (j == 0) {
    return std::string::npos;
  }
  const size_t atom_end = j;  // exclusive
  size_t atom_begin = 0;
  bool literal_atom = false;
  if (sql[j - 1] == '\'' || sql[j - 1] == '"' || sql[j - 1] == '`') {
    // Literal: the opening quote is the first char going backward whose
    // mask copy is not blanked-while-original-isn't.
    literal_atom = true;
    atom_begin = j - 1;
    while (atom_begin > 0 && mask[atom_begin] != sql[atom_begin]) {
      --atom_begin;
    }
    if (atom_begin > 0 || mask[atom_begin] == sql[atom_begin]) {
      // `atom_begin` sits on the boundary char before the opening quote.
      ++atom_begin;
    }
  } else if (sql[j - 1] == ')') {
    // Parenthesized expression (or call tail): match backward.
    size_t depth = 0;
    atom_begin = j - 1;
    while (true) {
      if (mask[atom_begin] == sql[atom_begin]) {  // outside literals
        if (sql[atom_begin] == ')') {
          ++depth;
        } else if (sql[atom_begin] == '(') {
          --depth;
          if (depth == 0) {
            break;
          }
        }
      }
      if (atom_begin == 0) {
        return std::string::npos;
      }
      --atom_begin;
    }
    // Function-call tail: absorb the callee name and its dotted path.
    while (atom_begin > 0 &&
           (IsIdentChar(sql[atom_begin - 1]) || sql[atom_begin - 1] == '.')) {
      --atom_begin;
    }
  } else if (IsIdentChar(sql[j - 1])) {
    atom_begin = j - 1;
    while (atom_begin > 0 && IsIdentChar(sql[atom_begin - 1])) {
      --atom_begin;
    }
    // Dotted path segments to the left (t.col).
    while (atom_begin > 1 && sql[atom_begin - 1] == '.' &&
           IsIdentChar(sql[atom_begin - 2])) {
      atom_begin -= 2;
      while (atom_begin > 0 && IsIdentChar(sql[atom_begin - 1])) {
        --atom_begin;
      }
    }
  } else {
    return std::string::npos;
  }
  // The atom must not be a bare keyword (SELECT ilike ... would otherwise
  // rewrite "SELECT"); literal atoms are exempt.
  if (!literal_atom) {
    std::string_view word(sql.data() + atom_begin, atom_end - atom_begin);
    std::string upper;
    upper.reserve(word.size());
    for (const char c : word) {
      upper.push_back(
          static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
    if (!IsIdentChar(sql[atom_begin]) || BoundaryKeywordBeforeOperand(upper)) {
      return std::string::npos;
    }
  }
  return atom_begin;
}

// Scans forward from `pos` (inclusive) over the atom starting there and
// returns one-past-the-end, or npos when the shape is not understood.
size_t LikeOperandEnd(std::string_view sql, std::string_view mask, size_t pos) {
  const size_t size = sql.size();
  size_t j = pos;
  while (j < size && IsSqlSpace(sql[j])) {
    ++j;
  }
  if (j >= size) {
    return std::string::npos;
  }
  const char c = sql[j];
  if (c == '\'' || c == '"' || c == '`') {
    // Scan the literal itself with its quoting rules. Comparing against the
    // mask (mask[j] != sql[j]) stopped at the first space *inside* a literal
    // such as 'a b%': MaskLiteralsAndComments blanks those bytes too, so the
    // right operand was truncated and LOWER(...) was left unbalanced. Match
    // the quote handling used by MaskLiteralsAndComments instead.
    const char quote = c;
    const bool triple = quote != '`' && j + 2 < size && sql[j + 1] == quote &&
                        sql[j + 2] == quote;
    j += triple ? 3 : 1;
    while (j < size) {
      if (triple && sql[j] == quote && j + 2 < size && sql[j + 1] == quote &&
          sql[j + 2] == quote) {
        return j + 3;
      }
      if (!triple && sql[j] == quote) {
        return j + 1;
      }
      if (sql[j] == '\\' && j + 1 < size) {
        ++j;
      }
      ++j;
    }
    return std::string::npos;
  }
  if (c == '(') {
    size_t depth = 0;
    while (j < size) {
      if (mask[j] == sql[j]) {  // outside literals
        if (sql[j] == '(') {
          ++depth;
        } else if (sql[j] == ')') {
          --depth;
          if (depth == 0) {
            return j + 1;
          }
        }
      }
      ++j;
    }
    return std::string::npos;
  }
  if (IsIdentChar(c)) {
    while (j < size && IsIdentChar(sql[j])) {
      ++j;
    }
    // Dotted path to the right (t.col).
    while (j + 1 < size && sql[j] == '.' && IsIdentChar(sql[j + 1])) {
      j += 2;
      while (j < size && IsIdentChar(sql[j])) {
        ++j;
      }
    }
    // Function call tail.
    if (j < size && sql[j] == '(') {
      return LikeOperandEnd(sql, mask, j);
    }
    return j;
  }
  return std::string::npos;
}

std::string NormalizeCaseInsensitiveLike(std::string_view sql) {
  const std::string mask = MaskLiteralsAndComments(sql);
  std::string lower_mask(mask);
  for (char& c : lower_mask) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  std::vector<size_t> matches;
  for (size_t pos = lower_mask.find("ilike"); pos != std::string::npos;
       pos = lower_mask.find("ilike", pos + 1)) {
    // Word-bounded on both sides (mask and sql agree at these positions).
    if (pos > 0 && (IsIdentChar(mask[pos - 1]) || mask[pos - 1] == '"')) {
      continue;
    }
    if (pos + 5 < mask.size() && IsIdentChar(mask[pos + 5])) {
      continue;
    }
    matches.push_back(pos);
  }
  if (matches.empty()) {
    return std::string(sql);
  }
  std::string result(sql);
  // Right-to-left so earlier offsets stay valid across rewrites.
  for (const size_t ilike_pos : std::ranges::reverse_view(matches)) {
    size_t operand_end = ilike_pos;
    bool negated = false;
    {
      // Optional NOT immediately before the left operand.
      size_t k = ilike_pos;
      while (k > 0 && IsSqlSpace(sql[k - 1])) {
        --k;
      }
      if (k >= 3) {
        std::string word(sql.substr(k - 3, 3));
        for (char& c : word) {
          c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
        const bool word_not = word == "NOT";
        const bool bounded_left =
            k == 3 || (!IsIdentChar(sql[k - 4]) && sql[k - 4] != '"' &&
                       sql[k - 4] != '\'');
        if (word_not && bounded_left) {
          negated = true;
          operand_end = k - 3;
        }
      }
    }
    const size_t left_begin = LikeOperandBegin(sql, mask, operand_end);
    if (left_begin == std::string::npos) {
      continue;
    }
    // The char before the left atom must be a structural boundary.
    size_t before = left_begin;
    while (before > 0 && IsSqlSpace(sql[before - 1])) {
      --before;
    }
    if (before > 0) {
      const char b = sql[before - 1];
      const bool operator_boundary =
          b == '(' || b == ',' || b == '=' || b == '<' || b == '>' ||
          b == '!' || b == '+' || b == '-' || b == '*' || b == '/';
      bool keyword_boundary = false;
      if (IsIdentChar(b)) {
        size_t word_end = before;
        size_t word_begin = before;
        while (word_begin > 0 && IsIdentChar(sql[word_begin - 1])) {
          --word_begin;
        }
        std::string word(sql.substr(word_begin, word_end - word_begin));
        for (char& c : word) {
          c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
        keyword_boundary = BoundaryKeywordBeforeOperand(word);
      }
      if (!operator_boundary && !keyword_boundary) {
        continue;
      }
    }
    const size_t right_begin = ilike_pos + 5;
    const size_t right_end = LikeOperandEnd(sql, mask, right_begin);
    if (right_end == std::string::npos) {
      continue;
    }
    // After the right atom the statement must continue at a structural
    // boundary (end, `;`, `,`, depth-0 close, or a clause keyword).
    size_t after = right_end;
    while (after < sql.size() && IsSqlSpace(sql[after])) {
      ++after;
    }
    if (after < sql.size()) {
      const char a = sql[after];
      bool structural = a == ';' || a == ',' || a == ')' || a == ']';
      if (!structural && IsIdentChar(a)) {
        size_t word_end = after;
        while (word_end < sql.size() && IsIdentChar(sql[word_end])) {
          ++word_end;
        }
        static constexpr std::array<std::string_view, 18> kFollowers = {
            "AND",   "OR",     "NOT",     "GROUP",     "ORDER",  "HAVING",
            "LIMIT", "OFFSET", "QUALIFY", "WINDOW",    "THEN",   "ELSE",
            "END",   "WHEN",   "UNION",   "INTERSECT", "EXCEPT", "AS"};
        std::string word(sql.substr(after, word_end - after));
        for (char& c : word) {
          c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
        for (const std::string_view follower : kFollowers) {
          if (word == follower) {
            structural = true;
            break;
          }
        }
      }
      if (!structural) {
        continue;
      }
    }
    const std::string left =
        result.substr(left_begin, operand_end - left_begin);
    const std::string right =
        result.substr(right_begin, right_end - right_begin);
    std::string rewritten = result.substr(0, left_begin);
    rewritten += "LOWER(";
    rewritten += left;
    rewritten += ")";
    rewritten += negated ? " NOT LIKE" : " LIKE";
    rewritten += " LOWER(";
    rewritten += right;
    rewritten += ")";
    rewritten += result.substr(right_end);
    result = std::move(rewritten);
  }
  return result;
}

std::string NormalizeCreateTableEngine(std::string_view sql) {
  const std::string mask = MaskLiteralsAndComments(sql);
  const std::string lower_mask = ToLowerCopy(mask);
  const size_t n = lower_mask.size();
  auto skip_ws = [&](size_t p) {
    while (p < n && IsSqlSpace(lower_mask[p])) {
      ++p;
    }
    return p;
  };
  auto read_word = [&](size_t p, std::string_view* out_word) -> size_t {
    p = skip_ws(p);
    if (p >= n || !IsIdentChar(lower_mask[p])) {
      return std::string::npos;
    }
    const size_t start = p;
    while (p < n && IsIdentChar(lower_mask[p])) {
      ++p;
    }
    *out_word = std::string_view(lower_mask.data() + start, p - start);
    return p;
  };

  size_t pos = 0;
  std::string_view word;
  pos = read_word(pos, &word);
  if (pos == std::string::npos || word != "create") {
    return std::string(sql);
  }
  while (true) {
    size_t next = read_word(pos, &word);
    if (next == std::string::npos) {
      return std::string(sql);
    }
    pos = next;
    if (word == "or" || word == "replace" || word == "temp" ||
        word == "temporary") {
      continue;
    }
    break;
  }
  if (word != "table") {
    return std::string(sql);
  }
  // Skip optional IF NOT EXISTS and table name path.
  pos = skip_ws(pos);
  {
    size_t probe = pos;
    std::string_view w1;
    std::string_view w2;
    std::string_view w3;
    size_t p1 = read_word(probe, &w1);
    if (p1 != std::string::npos && w1 == "function") {
      return std::string(sql);
    }
    if (p1 != std::string::npos && w1 == "if") {
      size_t p2 = read_word(p1, &w2);
      size_t p3 =
          p2 != std::string::npos ? read_word(p2, &w3) : std::string::npos;
      if (p3 != std::string::npos && w2 == "not" && w3 == "exists") {
        pos = p3;
      }
    }
  }
  pos = skip_ws(pos);
  // Advance past table path (ident, backtick-quoted ident, or dotted path).
  while (pos < n) {
    if (sql[pos] == '`') {
      ++pos;
      while (pos < n && sql[pos] != '`') {
        ++pos;
      }
      if (pos < n) {
        ++pos;
      }
    } else if (IsIdentChar(lower_mask[pos])) {
      while (pos < n && IsIdentChar(lower_mask[pos])) {
        ++pos;
      }
    } else {
      break;
    }
    pos = skip_ws(pos);
    if (pos < n && lower_mask[pos] == '.') {
      ++pos;
      pos = skip_ws(pos);
      continue;
    }
    break;
  }
  pos = skip_ws(pos);
  // Optional column definition list `(...)`.
  if (pos < n && lower_mask[pos] == '(') {
    int depth = 0;
    while (pos < n) {
      if (lower_mask[pos] == '(') {
        ++depth;
      } else if (lower_mask[pos] == ')') {
        --depth;
        if (depth == 0) {
          ++pos;
          break;
        }
      }
      ++pos;
    }
  }
  pos = skip_ws(pos);
  if (pos >= n) {
    return std::string(sql);
  }

  auto extract_token_value = [&](size_t p, std::string* out_val,
                                 size_t* out_end) -> bool {
    while (p < n && IsSqlSpace(sql[p])) {
      ++p;
    }
    if (p >= n) {
      return false;
    }
    if (sql[p] == '\'' || sql[p] == '"') {
      const char quote = sql[p];
      const size_t val_start = p + 1;
      ++p;
      while (p < n && sql[p] != quote) {
        if (sql[p] == '\\' && p + 1 < n) {
          p += 2;
          continue;
        }
        ++p;
      }
      if (p >= n) {
        return false;
      }
      *out_val = std::string(sql.substr(val_start, p - val_start));
      *out_end = p + 1;
      return true;
    }
    if (IsIdentChar(sql[p])) {
      const size_t val_start = p;
      while (p < n && IsIdentChar(sql[p])) {
        ++p;
      }
      *out_val = std::string(sql.substr(val_start, p - val_start));
      *out_end = p;
      return true;
    }
    return false;
  };

  const size_t clause_start = pos;
  std::string_view kw;
  const size_t after_kw = read_word(pos, &kw);
  if (after_kw == std::string::npos) {
    return std::string(sql);
  }
  if (kw == "using") {
    std::string engine_val;
    size_t clause_end = 0;
    if (extract_token_value(after_kw, &engine_val, &clause_end)) {
      std::string out(sql.substr(0, clause_start));
      out += "OPTIONS (engine = '";
      out += engine_val;
      out += "')";
      out += sql.substr(clause_end);
      return out;
    }
  } else if (kw == "engine" || kw == "storage_engine") {
    size_t p = skip_ws(after_kw);
    if (p < n && lower_mask[p] == '=') {
      ++p;
    }
    std::string engine_val;
    size_t clause_end = 0;
    if (extract_token_value(p, &engine_val, &clause_end)) {
      std::string out(sql.substr(0, clause_start));
      out += "OPTIONS (engine = '";
      out += engine_val;
      out += "')";
      out += sql.substr(clause_end);
      return out;
    }
  } else if (kw == "with" || kw == "options") {
    size_t p = skip_ws(after_kw);
    if (p < n && lower_mask[p] == '(') {
      ++p;
      std::string_view opt_key;
      size_t after_key = read_word(p, &opt_key);
      if (after_key != std::string::npos &&
          (opt_key == "engine" || opt_key == "storage_engine")) {
        size_t eq_pos = skip_ws(after_key);
        if (eq_pos < n && lower_mask[eq_pos] == '=') {
          ++eq_pos;
          std::string engine_val;
          // `clause_end` = end of the value token, same name as in the
          // `using` / `engine` arms above.
          size_t clause_end = 0;
          if (extract_token_value(eq_pos, &engine_val, &clause_end)) {
            size_t close_pos = skip_ws(clause_end);
            if (close_pos < n && lower_mask[close_pos] == ')') {
              std::string out(sql.substr(0, clause_start));
              out += "OPTIONS (engine = '";
              out += engine_val;
              out += "')";
              out += sql.substr(close_pos + 1);
              return out;
            }
          }
        }
      }
    }
  }
  return std::string(sql);
}

}  // namespace

GoogleSqlParseResult ParseViaSubprocess(std::string_view sql) {
  std::string current_sql = NormalizeCreateTableEngine(
      NormalizeCaseInsensitiveLike(NormalizeGroupByDistinct(sql)));

  size_t dist_start = 0;
  size_t dist_end = 0;
  std::string distinct_keys;
  if (ExtractDistinctOn(current_sql, &dist_start, &dist_end, &distinct_keys)) {
    std::string main_sql = current_sql.substr(0, dist_start) + "DISTINCT " +
                           current_sql.substr(dist_end);
    GoogleSqlParseResult main_res = ParseViaSubprocess(main_sql);
    if (!main_res.ok) {
      return main_res;
    }
    GoogleSqlParseResult keys_res =
        ParseViaSubprocess("SELECT " + distinct_keys + ";");
    if (!keys_res.ok) {
      // A malformed DISTINCT ON key list is a parse error, not a silent
      // downgrade to plain DISTINCT (which would run different semantics).
      return keys_res;
    }

    // Locate the SELECT keyword that owns this DISTINCT ON (the text before
    // dist_start is unchanged in main_sql, so its offset matches the dump
    // span of the owning Select node even when it is nested).
    std::optional<size_t> owner_select;
    {
      const std::string mask = ToLowerCopy(MaskLiteralsAndComments(main_sql));
      size_t p = dist_start;
      while (p > 0 &&
             std::isspace(static_cast<unsigned char>(mask[p - 1])) != 0) {
        --p;
      }
      if (p >= 6 && mask.compare(p - 6, 6, "select") == 0 &&
          (p == 6 ||
           (std::isalnum(static_cast<unsigned char>(mask[p - 7])) == 0 &&
            mask[p - 7] != '_'))) {
        owner_select = p - 6;
      }
    }
    if (!frontend_detail::InjectDistinctOnKeys(main_res.ast, keys_res.ast,
                                               owner_select)) {
      main_res.ok = false;
      main_res.ast.clear();
      main_res.error = "cannot preserve DISTINCT ON: unrecognized parser AST";
    }
    return main_res;
  }

  std::string rewritten_fetch;
  bool with_ties = false;
  if (ExtractFetchWithTies(current_sql, &rewritten_fetch, &with_ties)) {
    GoogleSqlParseResult res = ParseRawSubprocess(rewritten_fetch);
    if (!res.ok) {
      return res;
    }
    if (with_ties) {
      // The FETCH prepass rewrites only the outer clause. The AST dump is
      // depth-first, so its last LimitOffset can belong to a nested query.
      // Choose the shallowest LimitOffset node, which belongs to the outer
      // query being rewritten.
      size_t limit_pos = std::string::npos;
      size_t limit_indent = std::numeric_limits<size_t>::max();
      size_t line_start = 0;
      while (line_start < res.ast.size()) {
        const size_t line_end = res.ast.find('\n', line_start);
        const size_t end =
            line_end == std::string::npos ? res.ast.size() : line_end;
        const size_t indent = res.ast.find_first_not_of(' ', line_start);
        if (indent < end && res.ast.compare(indent, 11, "LimitOffset") == 0 &&
            indent - line_start < limit_indent) {
          limit_pos = indent;
          limit_indent = indent - line_start;
        }
        if (line_end == std::string::npos) {
          break;
        }
        line_start = line_end + 1;
      }
      if (limit_pos != std::string::npos) {
        size_t node_line_start = res.ast.rfind('\n', limit_pos);
        node_line_start =
            (node_line_start == std::string::npos) ? 0 : node_line_start + 1;
        size_t limit_line_end = res.ast.find('\n', limit_pos);
        if (limit_line_end != std::string::npos) {
          res.ast.insert(limit_line_end + 1,
                         std::string(limit_indent + 2, ' ') + "WithTies\n");
        }
      }
    }
    return res;
  }

  return ParseRawSubprocess(current_sql);
}

#endif  // TINYLAMB_GOOGLESQL_EXECUTABLE

}  // namespace

size_t GoogleSqlFrontend::CachedStatementCountForTest() {
#if defined(TINYLAMB_GOOGLESQL_EXECUTABLE) && \
    (defined(TINYLAMB_OS_POSIX) || defined(TINYLAMB_OS_WINDOWS))
  size_t total = 0;
  for (ParseShard& shard : parse_shards) {
    std::scoped_lock lock(shard.mutex);
    total += shard.cache.size();
  }
  return total;
#else
  return 0;
#endif
}

bool frontend_detail::InjectDistinctOnKeys(
    std::string& ast, const std::string& keys_ast,
    std::optional<size_t> select_offset) {
  size_t select_pos = std::string::npos;
  if (select_offset.has_value()) {
    // Nested DISTINCT ON (derived table, CTE body, UNION branch): attach the
    // keys to exactly the Select whose source span starts at the owning
    // SELECT keyword, never to whichever Select the dump lists first.
    // Dump lines look like "Select [s-e]" or "Select(distinct=true) [s-e]".
    const std::string span = " [" + std::to_string(*select_offset) + "-";
    for (size_t at = ast.find(span); at != std::string::npos;
         at = ast.find(span, at + 1)) {
      size_t line_start = ast.rfind('\n', at);
      line_start = line_start == std::string::npos ? 0 : line_start + 1;
      while (line_start < at && ast[line_start] == ' ') {
        ++line_start;
      }
      const std::string_view head(ast.data() + line_start, at - line_start);
      if (head == "Select" ||
          (head.starts_with("Select(") && head.ends_with(")"))) {
        select_pos = line_start;
        break;
      }
    }
    if (select_pos == std::string::npos) {
      return false;
    }
  }
  for (const char* pattern : {"Select(", "Select [", "Select\n", "Select "}) {
    if (select_pos != std::string::npos) {
      break;
    }
    size_t found = ast.find(pattern);
    if (found != std::string::npos) {
      select_pos = found;
      break;
    }
  }
  if (select_pos != std::string::npos) {
    size_t sel_line_start = ast.rfind('\n', select_pos);
    sel_line_start =
        (sel_line_start == std::string::npos) ? 0 : sel_line_start + 1;
    size_t select_indent = 0;
    while (sel_line_start + select_indent < ast.size() &&
           ast[sel_line_start + select_indent] == ' ') {
      ++select_indent;
    }

    bool has_key = false;
    std::string distinct_on_block =
        std::string(select_indent + 2, ' ') + "DistinctOn\n";
    size_t select_list_pos = keys_ast.find("SelectList");
    if (select_list_pos != std::string::npos) {
      size_t sl_line_start = keys_ast.rfind('\n', select_list_pos);
      sl_line_start =
          (sl_line_start == std::string::npos) ? 0 : sl_line_start + 1;
      size_t sl_indent = 0;
      while (sl_line_start + sl_indent < keys_ast.size() &&
             keys_ast[sl_line_start + sl_indent] == ' ') {
        ++sl_indent;
      }

      size_t next_line = keys_ast.find('\n', select_list_pos);
      if (next_line != std::string::npos) {
        ++next_line;
        while (next_line < keys_ast.size()) {
          size_t cur_end = keys_ast.find('\n', next_line);
          if (cur_end == std::string::npos) {
            cur_end = keys_ast.size();
          }
          std::string line = keys_ast.substr(next_line, cur_end - next_line);
          size_t sp = 0;
          while (sp < line.size() && line[sp] == ' ') {
            ++sp;
          }
          if (sp <= sl_indent && !line.empty()) {
            break;
          }
          if (!line.empty()) {
            has_key = true;
            size_t rel = sp - sl_indent;
            distinct_on_block += std::string(select_indent + 2 + rel, ' ') +
                                 line.substr(sp) + "\n";
          }
          next_line = cur_end + 1;
        }
      }
    }

    size_t select_line_end = ast.find('\n', select_pos);
    if (select_line_end != std::string::npos && has_key) {
      ast.insert(select_line_end + 1, distinct_on_block);
      return true;
    }
  }
  return false;
}

bool GoogleSqlFrontend::Available() {
#if defined(TINYLAMB_GOOGLESQL_EXECUTABLE) && defined(TINYLAMB_OS_POSIX)
  // Use the same runtime-resolved path as spawns (env override / installed
  // binary); the compile-time default may not exist in the current tree.
  return access(ResolveGoogleSqlExecutable().c_str(), X_OK) == 0;
#elif defined(TINYLAMB_GOOGLESQL_EXECUTABLE) && defined(TINYLAMB_OS_WINDOWS)
  std::error_code ec;
  return std::filesystem::exists(ResolveGoogleSqlExecutable(), ec);
#else
  return false;
#endif
}

GoogleSqlParseResult GoogleSqlFrontend::Parse(std::string_view sql) {
#if defined(TINYLAMB_GOOGLESQL_EXECUTABLE) && \
    (defined(TINYLAMB_OS_POSIX) || defined(TINYLAMB_OS_WINDOWS))
  const std::string cache_key(sql);
  ParseShard& shard = ShardFor(cache_key);
  std::unique_lock cache_lock(shard.mutex);
  shard.parse_finished.wait(cache_lock,
                            [&] { return !shard.pending.contains(cache_key); });
  const size_t max_per_shard = MaxCachedStatementsPerShard();
  const auto neg_ttl = NegativeCacheTtlSeconds();
  const auto now = std::chrono::steady_clock::now();
  {
    const auto cached = shard.cache.find(cache_key);
    if (cached != shard.cache.end()) {
      return {.ok = true, .ast = cached->second, .error = {}};
    }
    const auto failed = shard.negative_cache.find(cache_key);
    if (failed != shard.negative_cache.end()) {
      if (now - failed->second < neg_ttl) {
        return {.ok = false,
                .ast = {},
                .error = "GoogleSQL parse failed (recently cached)"};
      }
      shard.negative_key_bytes -= failed->first.size();
      shard.negative_cache.erase(failed);
    }
  }

  // The view in pending points into this call's key and is erased before the
  // key goes out of scope, including when parsing or cache insertion throws.
  shard.pending.insert(cache_key);
  const UnwindGuard finish_parse([&] {
    if (!cache_lock.owns_lock()) {
      cache_lock.lock();
    }
    shard.pending.erase(cache_key);
    cache_lock.unlock();
    shard.parse_finished.notify_all();
  });
  cache_lock.unlock();
  GoogleSqlParseResult parsed;
  {
    std::scoped_lock process_slot(parse_process_slots);
    parsed = ParseViaSubprocess(sql);
  }
  if (cache_key.size() > kMaxParseCacheKeyBytesPerShard) {
    return parsed;
  }
  cache_lock.lock();
  if (parsed.ok) {
    // unordered_map iteration order is unspecified, so evict an ARBITRARY
    // entry until both the count and key-byte bounds hold.
    const size_t spare = kMaxParseCacheKeyBytesPerShard - cache_key.size();
    while (shard.cache.size() >= max_per_shard ||
           shard.cached_key_bytes > spare) {
      auto victim = shard.cache.begin();
      shard.cached_key_bytes -= victim->first.size();
      shard.cache.erase(victim);
    }
    shard.cache.emplace(cache_key, parsed.ast);
    shard.cached_key_bytes += cache_key.size();
  } else {
    // Negative-cache the parser's explicit syntax verdict. A diagnostic about
    // a resource failure may also start with "ERROR:" and need an immediate
    // retry after the resource becomes available.
    const std::string_view diagnostic = ParserErrorLine(parsed.error);
    if (!parsed.retryable &&
        (diagnostic.starts_with("ERROR: INVALID_ARGUMENT: Syntax error:") ||
         diagnostic.starts_with("ERROR: Syntax error:"))) {
      const size_t spare = kMaxParseCacheKeyBytesPerShard - cache_key.size();
      while (shard.negative_cache.size() >= max_per_shard ||
             shard.negative_key_bytes > spare) {
        const auto victim = shard.negative_cache.begin();
        shard.negative_key_bytes -= victim->first.size();
        shard.negative_cache.erase(victim);
      }
      shard.negative_cache.emplace(cache_key, std::chrono::steady_clock::now());
      shard.negative_key_bytes += cache_key.size();
    }
  }
  return parsed;
#else
  (void)sql;
  return {.ok = false,
          .ast = {},
          .error = "GoogleSQL AST support is unavailable on this platform"};
#endif
}

}  // namespace tinylamb
