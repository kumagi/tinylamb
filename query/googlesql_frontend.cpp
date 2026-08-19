/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "query/googlesql_frontend.hpp"

#include <cerrno>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#if defined(TINYLAMB_GOOGLESQL_EXECUTABLE) && defined(__unix__)
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace tinylamb {
namespace {

std::shared_mutex parse_cache_mutex;
std::unordered_map<std::string, std::string> parse_cache;
constexpr size_t kMaxCachedStatements = 1024;

}  // namespace

bool GoogleSqlFrontend::Available() {
#if defined(TINYLAMB_GOOGLESQL_EXECUTABLE) && defined(__unix__)
  return access(TINYLAMB_GOOGLESQL_EXECUTABLE, X_OK) == 0;
#else
  return false;
#endif
}

GoogleSqlParseResult GoogleSqlFrontend::Parse(std::string_view sql) {
#if defined(TINYLAMB_GOOGLESQL_EXECUTABLE) && defined(__unix__)
  // The parser is an external process. Apart from avoiding repeated fork/exec
  // overhead for OLTP statements, serializing cache misses prevents a burst
  // of worker threads from exhausting the process limit. AST dumps are
  // immutable and parsing them into tinylamb nodes remains per-query.
  const std::string cache_key(sql);
  {
    std::shared_lock cache_lock(parse_cache_mutex);
    const auto cached = parse_cache.find(cache_key);
    if (cached != parse_cache.end()) return {true, cached->second, {}};
  }
  std::unique_lock cache_lock(parse_cache_mutex);
  // Another worker may have populated it while this worker waited.
  const auto cached = parse_cache.find(cache_key);
  if (cached != parse_cache.end()) return {true, cached->second, {}};

  int input_pipe[2];
  int output_pipe[2];
  if (pipe(input_pipe) != 0 || pipe(output_pipe) != 0) {
    return {
        false,
        {},
        std::string("cannot create GoogleSQL pipe: ") + std::strerror(errno)};
  }

  const pid_t child = fork();
  if (child < 0) {
    close(input_pipe[0]);
    close(input_pipe[1]);
    close(output_pipe[0]);
    close(output_pipe[1]);
    return {false,
            {},
            std::string("cannot start GoogleSQL: ") + std::strerror(errno)};
  }
  if (child == 0) {
    dup2(input_pipe[0], STDIN_FILENO);
    dup2(output_pipe[1], STDOUT_FILENO);
    dup2(output_pipe[1], STDERR_FILENO);
    close(input_pipe[0]);
    close(input_pipe[1]);
    close(output_pipe[0]);
    close(output_pipe[1]);
    execl(TINYLAMB_GOOGLESQL_EXECUTABLE, TINYLAMB_GOOGLESQL_EXECUTABLE,
          "--mode=parse", "-", static_cast<char*>(nullptr));
    _exit(127);
  }

  close(input_pipe[0]);
  close(output_pipe[1]);
  size_t written = 0;
  while (written < sql.size()) {
    const ssize_t count =
        write(input_pipe[1], sql.data() + written, sql.size() - written);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    written += static_cast<size_t>(count);
  }
  close(input_pipe[1]);

  std::string output;
  char buffer[4096];
  for (;;) {
    const ssize_t count = read(output_pipe[0], buffer, sizeof(buffer));
    if (count == 0) {
      break;
    }
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    output.append(buffer, static_cast<size_t>(count));
  }
  close(output_pipe[0]);

  int status = 0;
  while (waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    return {false, {}, std::move(output)};
  }
  if (output.starts_with("ERROR:")) {
    return {false, {}, std::move(output)};
  }
  if (parse_cache.size() >= kMaxCachedStatements) {
    parse_cache.erase(parse_cache.begin());
  }
  parse_cache.emplace(cache_key, output);
  return {true, std::move(output), {}};
#else
  return {false, {}, "GoogleSQL AST support is unavailable on this platform"};
#endif
}

}  // namespace tinylamb
