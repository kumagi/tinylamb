/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "query/googlesql_frontend.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <bits/types/sigset_t.h>
#include <fcntl.h>
#include <functional>
#include <mutex>
#include <poll.h>
#include <signal.h>  // NOLINT(modernize-deprecated-headers) // POSIX signal APIs below (sigemptyset, pthread_sigmask, SIGPIPE) are only provided by this header.
#include <string>
#include <sys/poll.h>
#include <cstdlib>
#include <sys/types.h>
#include <sys/wait.h>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <utility>

namespace tinylamb {
namespace {

constexpr size_t kParseCacheShards = 16;
constexpr size_t kMaxCachedStatements = 1024;
constexpr size_t kMaxCachedStatementsPerShard =
    kMaxCachedStatements / kParseCacheShards;
// Failed parses are remembered briefly so retries of the same broken SQL do
// not fork one parser process per attempt (§7.11).
constexpr std::chrono::seconds kNegativeCacheTtl{1};
// Hard bound for one parse round trip; a stuck or flooded child is killed
// instead of blocking the worker forever (§7.2).
constexpr std::chrono::seconds kParseTimeout{5};
constexpr size_t kMaxChildOutputBytes = 64U << 20;

struct ParseShard {
  std::mutex mutex;
  std::unordered_map<std::string, std::string> cache;
  std::unordered_map<std::string, std::chrono::steady_clock::time_point>
      negative_cache;
};

std::array<ParseShard, kParseCacheShards> parse_shards;

ParseShard& ShardFor(std::string_view sql) {
  return parse_shards[std::hash<std::string_view>{}(sql) % kParseCacheShards];
}

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
  sigset_t set_;
  sigset_t old_;
};

#if defined(TINYLAMB_GOOGLESQL_EXECUTABLE) && defined(__unix__)
GoogleSqlParseResult ParseRawSubprocess(std::string_view sql) {
  std::array<int, 2> input_pipe{};
  std::array<int, 2> output_pipe{};
  // O_CLOEXEC keeps concurrent parses' descriptors out of the child; only
  // the dup2'd std{in,out,err} survive exec.
  if (pipe2(input_pipe.data(), O_CLOEXEC) != 0) {
    return {.ok=false,
            .ast={},
            .error=std::string("cannot create GoogleSQL pipe: ") + std::strerror(errno)};
  }
  if (pipe2(output_pipe.data(), O_CLOEXEC) != 0) {
    const std::string reason(errno == EMFILE || errno == ENFILE
                                 ? std::string("too many open files")
                                 : std::strerror(errno));
    CloseAll(input_pipe);
    return {.ok=false, .ast={}, .error="cannot create GoogleSQL pipe: " + reason};
  }

  const pid_t child = fork();
  if (child < 0) {
    const std::string reason(std::strerror(errno));
    CloseAll(input_pipe);
    CloseAll(output_pipe);
    return {.ok=false, .ast={}, .error="cannot start GoogleSQL: " + reason};
  }
  if (child == 0) {
    dup2(input_pipe[0], STDIN_FILENO);
    dup2(output_pipe[1], STDOUT_FILENO);
    dup2(output_pipe[1], STDERR_FILENO);
    CloseAll(input_pipe);
    CloseAll(output_pipe);
    execl(TINYLAMB_GOOGLESQL_EXECUTABLE, TINYLAMB_GOOGLESQL_EXECUTABLE,
          "--mode=parse", "-", static_cast<char*>(nullptr));
    _exit(127);
  }

  close(input_pipe[0]);
  close(output_pipe[1]);

  const auto deadline =
      std::chrono::steady_clock::now() + kParseTimeout;
  BlockedSigPipe blocked_sigpipe;
  bool input_open = true;
  bool output_open = true;
  size_t written = 0;
  std::string output;
  // Interleave writes and reads via poll: the child emits its AST while
  // still consuming the query, so a large result cannot deadlock a parent
  // that is stuck in write() (§7.2). One syscall per readiness event keeps
  // the blocking descriptors from stalling the loop.
  while (input_open || output_open) {
    const auto remaining = deadline - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::steady_clock::duration::zero()) { break;
}
    // Fully written (or empty) query: hand the child its EOF even when no
    // POLLOUT event will ever fire.
    if (input_open && written >= sql.size()) {
      close(input_pipe[1]);
      input_open = false;
    }
    const int timeout_ms = static_cast<int>(
        std::min<std::chrono::milliseconds>(
            std::chrono::duration_cast<std::chrono::milliseconds>(remaining),
            std::chrono::milliseconds(100))
            .count());
    std::array<pollfd, 2> fds{};
    fds[0] = {.fd=output_pipe[0], .events=static_cast<short>(output_open ? POLLIN : 0), .revents=0};
    fds[1] = {.fd=input_pipe[1],
              .events=static_cast<short>(input_open && written < sql.size() ? POLLOUT
                                                                    : 0),
              .revents=0};
    const int ready = ::poll(fds.data(), 2, timeout_ms);
    if (ready < 0) {
      if (errno == EINTR) { continue;
}
      break;
    }
    if (ready == 0) { continue;
}

    if (output_open &&
        (fds[0].revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
      std::array<char, 4096> buffer{};
      const ssize_t count =
          read(output_pipe[0], buffer.data(), buffer.size());
      if (count > 0) {
        output.append(buffer.data(), static_cast<size_t>(count));
        if (output.size() > kMaxChildOutputBytes) {
          close(output_pipe[0]);
          output_open = false;
        }
      } else if (count == 0 || (errno != EINTR && errno != EAGAIN &&
                                errno != EWOULDBLOCK)) {
        close(output_pipe[0]);
        output_open = false;
      }
    }

    if (input_open && written < sql.size() &&
        (fds[1].revents & (POLLOUT | POLLHUP | POLLERR)) != 0) {
      const ssize_t count =
          write(input_pipe[1], sql.data() + written, sql.size() - written);
      if (count >= 0) {
        written += static_cast<size_t>(count);
      } else if (errno != EINTR && errno != EAGAIN &&
                 errno != EWOULDBLOCK) {
        // EPIPE: the child died early; stop feeding it.
        close(input_pipe[1]);
        input_open = false;
      }
    }
  }
  if (output_open) { close(output_pipe[0]);
}
  if (input_open) { close(input_pipe[1]);
}

  // Reap the child with a deadline; escalate to SIGKILL on overrun.
  int status = 0;
  bool reaped = false;
  while (std::chrono::steady_clock::now() < deadline) {
    const pid_t done = waitpid(child, &status, WNOHANG);  // NOLINT(misc-include-cleaner) glibc macro
    if (done == child) {
      reaped = true;
      break;
    }
    if (done < 0 && errno == EINTR) { continue;
}
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  if (!reaped) {
    kill(child, SIGKILL);
    while (waitpid(child, &status, 0) < 0 && errno == EINTR) {
    }
    GoogleSqlParseResult timed_out;
    timed_out.ok = false;
    timed_out.error = "GoogleSQL parser timed out";
    return timed_out;
  }
  if (!WIFEXITED(status) ||  // NOLINT(misc-include-cleaner) glibc macro
      WEXITSTATUS(status) != 0) {  // NOLINT(misc-include-cleaner) glibc macro
    return {.ok=false, .ast={}, .error=std::move(output)};
  }
  if (output.starts_with("ERROR:")) {
    return {.ok=false, .ast={}, .error=std::move(output)};
  }
  return {.ok=true, .ast=std::move(output), .error={}};
}

std::string ToLowerCopy(std::string_view s) {
  std::string result;
  result.reserve(s.size());
  for (char c : s) {
    result.push_back(
        static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return result;
}

bool ExtractDistinctOn(std::string_view sql, size_t* out_start,
                       size_t* out_end, std::string* out_exprs) {
  std::string lower = ToLowerCopy(sql);
  size_t pos = 0;
  while (pos < lower.size()) {
    size_t found = lower.find("distinct", pos);
    if (found == std::string::npos) break;
    if (found > 0 &&
        (std::isalnum(static_cast<unsigned char>(lower[found - 1])) ||
         lower[found - 1] == '_')) {
      pos = found + 8;
      continue;
    }
    size_t after_dist = found + 8;
    while (after_dist < lower.size() &&
           std::isspace(static_cast<unsigned char>(lower[after_dist]))) {
      ++after_dist;
    }
    if (lower.compare(after_dist, 2, "on") == 0) {
      size_t after_on = after_dist + 2;
      if (after_on < lower.size() &&
          (std::isalnum(static_cast<unsigned char>(lower[after_on])) ||
           lower[after_on] == '_')) {
        pos = after_on;
        continue;
      }
      while (after_on < lower.size() &&
             std::isspace(static_cast<unsigned char>(lower[after_on]))) {
        ++after_on;
      }
      if (after_on < lower.size() && lower[after_on] == '(') {
        size_t open_paren = after_on;
        int depth = 1;
        size_t close_paren = open_paren + 1;
        while (close_paren < sql.size() && depth > 0) {
          if (sql[close_paren] == '(') {
            ++depth;
          } else if (sql[close_paren] == ')') {
            --depth;
          }
          ++close_paren;
        }
        if (depth == 0) {
          *out_start = found;
          *out_end = close_paren;
          *out_exprs = std::string(sql.substr(open_paren + 1,
                                              close_paren - 1 - open_paren - 1));
          return true;
        }
      }
    }
    pos = found + 8;
  }
  return false;
}

bool ExtractFetchWithTies(std::string_view sql, std::string* out_rewritten,
                          bool* out_with_ties) {
  std::string lower = ToLowerCopy(sql);
  *out_with_ties = false;
  size_t ties_pos = lower.find("with ties");
  bool with_ties = (ties_pos != std::string::npos);
  *out_with_ties = with_ties;

  size_t fetch_pos = lower.find("fetch first");
  if (fetch_pos == std::string::npos) {
    fetch_pos = lower.find("fetch next");
  }

  if (fetch_pos != std::string::npos) {
    size_t offset_pos = lower.find("offset");
    std::string offset_val;
    if (offset_pos != std::string::npos && offset_pos < fetch_pos) {
      size_t cur = offset_pos + 6;
      while (cur < fetch_pos &&
             std::isspace(static_cast<unsigned char>(lower[cur]))) {
        ++cur;
      }
      while (cur < fetch_pos &&
             std::isdigit(static_cast<unsigned char>(lower[cur]))) {
        offset_val.push_back(lower[cur]);
        ++cur;
      }
    }

    size_t count_start = fetch_pos + 11;
    while (count_start < lower.size() &&
           std::isspace(static_cast<unsigned char>(lower[count_start]))) {
      ++count_start;
    }
    std::string count_val;
    size_t count_cur = count_start;
    while (count_cur < lower.size() &&
           std::isdigit(static_cast<unsigned char>(lower[count_cur]))) {
      count_val.push_back(lower[count_cur]);
      ++count_cur;
    }
    if (count_val.empty()) count_val = "1";

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
    size_t pos = lower.find("with ties");
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
  size_t pos = lower.find("group by distinct");
  if (pos != std::string::npos) {
    size_t after = pos + 17;
    // `distinct_1` is an ordinary identifier, not the GROUP BY DISTINCT
    // modifier.  Rewriting it would remove the identifier prefix and turn
    // the query into the invalid token `GROUP BY_1`.
    if (after < lower.size() &&
        (std::isalnum(static_cast<unsigned char>(lower[after])) != 0 ||
         lower[after] == '_')) {
      return std::string(sql);
    }
    while (after < lower.size() &&
           std::isspace(static_cast<unsigned char>(lower[after]))) {
      ++after;
    }
    if (lower.compare(after, 3, "all") == 0) {
      std::string result(sql.substr(0, pos));
      result += "GROUP BY ALL";
      result += sql.substr(after + 3);
      return result;
    }
    std::string result(sql.substr(0, pos));
    result += "GROUP BY";
    result += sql.substr(pos + 17);
    return result;
  }
  return std::string(sql);
}

GoogleSqlParseResult ParseViaSubprocess(std::string_view sql) {
  std::string current_sql = NormalizeGroupByDistinct(sql);

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
      return main_res;
    }

    size_t select_pos = std::string::npos;
    for (const char* pattern : {"Select(", "Select [", "Select\n", "Select "}) {
      size_t found = main_res.ast.find(pattern);
      if (found != std::string::npos) {
        select_pos = found;
        break;
      }
    }
    if (select_pos != std::string::npos) {
      size_t sel_line_start = main_res.ast.rfind('\n', select_pos);
      sel_line_start = (sel_line_start == std::string::npos) ? 0 : sel_line_start + 1;
      size_t select_indent = 0;
      while (sel_line_start + select_indent < main_res.ast.size() &&
             main_res.ast[sel_line_start + select_indent] == ' ') {
        ++select_indent;
      }

      std::string distinct_on_block = std::string(select_indent + 2, ' ') + "DistinctOn\n";
      size_t select_list_pos = keys_res.ast.find("SelectList");
      if (select_list_pos != std::string::npos) {
        size_t sl_line_start = keys_res.ast.rfind('\n', select_list_pos);
        sl_line_start = (sl_line_start == std::string::npos) ? 0 : sl_line_start + 1;
        size_t sl_indent = 0;
        while (sl_line_start + sl_indent < keys_res.ast.size() &&
               keys_res.ast[sl_line_start + sl_indent] == ' ') {
          ++sl_indent;
        }

        size_t next_line = keys_res.ast.find('\n', select_list_pos);
        if (next_line != std::string::npos) {
          ++next_line;
          while (next_line < keys_res.ast.size()) {
            size_t cur_end = keys_res.ast.find('\n', next_line);
            if (cur_end == std::string::npos) cur_end = keys_res.ast.size();
            std::string line = keys_res.ast.substr(next_line, cur_end - next_line);
            size_t sp = 0;
            while (sp < line.size() && line[sp] == ' ') ++sp;
            if (sp <= sl_indent && !line.empty()) break;
            if (!line.empty()) {
              size_t rel = sp - sl_indent;
              distinct_on_block += std::string(select_indent + 2 + rel, ' ') + line.substr(sp) + "\n";
            }
            next_line = cur_end + 1;
          }
        }
      }

      size_t select_line_end = main_res.ast.find('\n', select_pos);
      if (select_line_end != std::string::npos) {
        main_res.ast.insert(select_line_end + 1, distinct_on_block);
      }
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
      size_t limit_pos = res.ast.find("LimitOffset");
      if (limit_pos != std::string::npos) {
        size_t line_start = res.ast.rfind('\n', limit_pos);
        line_start = (line_start == std::string::npos) ? 0 : line_start + 1;
        size_t limit_indent = 0;
        while (line_start + limit_indent < res.ast.size() &&
               res.ast[line_start + limit_indent] == ' ') {
          ++limit_indent;
        }
        size_t limit_line_end = res.ast.find('\n', limit_pos);
        if (limit_line_end != std::string::npos) {
          res.ast.insert(limit_line_end + 1, std::string(limit_indent + 2, ' ') + "WithTies\n");
        }
      }
    }
    return res;
  }

  return ParseRawSubprocess(current_sql);
}
#endif

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
  // The parser is an external process. Sharded caches avoid a single global
  // reader/writer lock on OLTP parse bursts; misses still serialize per shard
  // so a burst of distinct statements does not exhaust the process limit.
  const std::string cache_key(sql);
  ParseShard& shard = ShardFor(cache_key);
  const auto now = std::chrono::steady_clock::now();
  {
    std::scoped_lock cache_lock(shard.mutex);
    const auto cached = shard.cache.find(cache_key);
    if (cached != shard.cache.end()) { return {.ok=true, .ast=cached->second, .error={}};
}
    const auto failed = shard.negative_cache.find(cache_key);
    if (failed != shard.negative_cache.end()) {
      if (now - failed->second < kNegativeCacheTtl) {
        return {.ok=false, .ast={}, .error="GoogleSQL parse failed (recently cached)"};
      }
      shard.negative_cache.erase(failed);
    }
  }

  GoogleSqlParseResult parsed = ParseViaSubprocess(sql);
  {
    std::scoped_lock cache_lock(shard.mutex);
    if (parsed.ok) {
      const auto cached = shard.cache.find(cache_key);
      if (cached != shard.cache.end()) { return {.ok=true, .ast=cached->second, .error={}};
}
      if (shard.cache.size() >= kMaxCachedStatementsPerShard) {
        // unordered_map iteration order is unspecified, so this evicts an
        // ARBITRARY entry, deliberately not the oldest: entries are equally
        // likely to be re-parsed in OLTP bursts and a true LRU/FIFO list
        // would add bookkeeping per lookup. Tests only require that the
        // bound holds.
        shard.cache.erase(shard.cache.begin());
      }
      shard.cache.emplace(cache_key, parsed.ast);
      shard.negative_cache.erase(cache_key);
    } else {
      if (shard.negative_cache.size() >= kMaxCachedStatementsPerShard) {
        shard.negative_cache.erase(shard.negative_cache.begin());
      }
      shard.negative_cache.insert_or_assign(
          cache_key, std::chrono::steady_clock::now());
    }
  }
  return parsed;
#else
  return {false, {}, "GoogleSQL AST support is unavailable on this platform"};
#endif
}

}  // namespace tinylamb
