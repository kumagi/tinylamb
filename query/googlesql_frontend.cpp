/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "query/googlesql_frontend.hpp"

#include <bits/types/sigset_t.h>
#include <fcntl.h>
#include <poll.h>
// NOLINTNEXTLINE(modernize-deprecated-headers) POSIX sigset_t APIs
#include <signal.h>
#include <sys/poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <mutex>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

namespace tinylamb {
namespace {

// Everything in this anonymous namespace is only reachable when the external
// GoogleSQL parser is wired in; guard the definitions themselves so builds
// without TINYLAMB_GOOGLESQL_EXECUTABLE stay warning-clean.
#if defined(TINYLAMB_GOOGLESQL_EXECUTABLE) && defined(__unix__)

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

// Tests that exercise EINTR retry on multi-MB inputs under sanitizer
// slowdown need a larger round-trip budget than the production default.
std::chrono::seconds ParseTimeoutSeconds() {
  if (const char* env = std::getenv("TINYLAMB_PARSE_TIMEOUT_SEC")) {
    const std::string owned(env);
    char* end = nullptr;
    const long value = std::strtol(owned.c_str(), &end, 10);
    if (value > 0 && end != nullptr && *end == '\0') {
      return std::chrono::seconds(value);
    }
  }
  return kParseTimeout;
}

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
  sigset_t set_{};
  sigset_t old_{};
};

GoogleSqlParseResult ParseRawSubprocess(std::string_view sql) {
  std::array<int, 2> input_pipe{};
  std::array<int, 2> output_pipe{};
  // O_CLOEXEC keeps concurrent parses' descriptors out of the child; only
  // the dup2'd std{in,out,err} survive exec.
  if (pipe2(input_pipe.data(), O_CLOEXEC) != 0) {
    return {.ok = false,
            .ast = {},
            .error = std::string("cannot create GoogleSQL pipe: ") +
                     std::strerror(errno)};
  }
  if (pipe2(output_pipe.data(), O_CLOEXEC) != 0) {
    const std::string reason(errno == EMFILE || errno == ENFILE
                                 ? std::string("too many open files")
                                 : std::strerror(errno));
    CloseAll(input_pipe);
    return {.ok = false,
            .ast = {},
            .error = "cannot create GoogleSQL pipe: " + reason};
  }

  const pid_t child = fork();
  if (child < 0) {
    const std::string reason(std::strerror(errno));
    CloseAll(input_pipe);
    CloseAll(output_pipe);
    return {
        .ok = false, .ast = {}, .error = "cannot start GoogleSQL: " + reason};
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
      std::chrono::steady_clock::now() + ParseTimeoutSeconds();
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
    if (remaining <= std::chrono::steady_clock::duration::zero()) {
      break;
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
    fds[0] = {.fd = output_pipe[0],
              .events = static_cast<short>(output_open ? POLLIN : 0),
              .revents = 0};
    fds[1] = {.fd = input_pipe[1],
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

    if (output_open && (fds[0].revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
      std::array<char, 4096> buffer{};
      // Blocking syscalls under the caller's parse-cache shard mutex are the
      // documented per-shard miss serialization; every syscall here is
      // poll-driven with a deadline, so the critical section stays bounded.
      // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection)
      const ssize_t count = read(output_pipe[0], buffer.data(), buffer.size());
      if (count > 0) {
        output.append(buffer.data(), static_cast<size_t>(count));
        if (output.size() > kMaxChildOutputBytes) {
          close(output_pipe[0]);
          output_open = false;
        }
      } else if (count == 0 ||
                 (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)) {
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
      } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
        // EPIPE: the child died early; stop feeding it.
        close(input_pipe[1]);
        input_open = false;
      }
    }
  }
  if (output_open) {
    close(output_pipe[0]);
  }
  if (input_open) {
    close(input_pipe[1]);
  }

  // Reap the child with a deadline; escalate to SIGKILL on overrun.
  int status = 0;
  bool reaped = false;
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
  if (!WIFEXITED(status) ||        // NOLINT(misc-include-cleaner) glibc macro
      WEXITSTATUS(status) != 0) {  // NOLINT(misc-include-cleaner) glibc macro
    return {.ok = false, .ast = {}, .error = std::move(output)};
  }
  if (output.starts_with("ERROR:")) {
    return {.ok = false, .ast = {}, .error = std::move(output)};
  }
  return {.ok = true, .ast = std::move(output), .error = {}};
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
        // A doubled quote is an escaped quote, not the terminator.
        if (sql[i] == quote && i + 1 < sql.size() && sql[i + 1] == quote) {
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
    if (found > 0 &&
        ((std::isalnum(static_cast<unsigned char>(lower[found - 1])) != 0) ||
         lower[found - 1] == '_')) {
      pos = found + 8;
      continue;
    }
    size_t after_dist = found + 8;
    while (after_dist < lower.size() &&
           (std::isspace(static_cast<unsigned char>(lower[after_dist])) != 0)) {
      ++after_dist;
    }
    if (lower.compare(after_dist, 2, "on") == 0) {
      size_t after_on = after_dist + 2;
      if (after_on < lower.size() &&
          ((std::isalnum(static_cast<unsigned char>(lower[after_on])) != 0) ||
           lower[after_on] == '_')) {
        pos = after_on;
        continue;
      }
      while (after_on < lower.size() &&
             (std::isspace(static_cast<unsigned char>(lower[after_on])) != 0)) {
        ++after_on;
      }
      if (after_on < lower.size() && lower[after_on] == '(') {
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

bool ExtractFetchWithTies(std::string_view sql, std::string* out_rewritten,
                          bool* out_with_ties) {
  // Keyword positions come from the literal/comment mask so that a "with
  // ties" / "fetch first" inside a string literal or comment is ignored.
  const std::string mask = ToLowerCopy(MaskLiteralsAndComments(sql));
  std::string lower = ToLowerCopy(sql);
  *out_with_ties = false;
  size_t ties_pos = mask.find("with ties");
  bool with_ties = (ties_pos != std::string::npos);
  *out_with_ties = with_ties;

  size_t fetch_pos = mask.find("fetch first");
  if (fetch_pos == std::string::npos) {
    fetch_pos = mask.find("fetch next");
  }

  if (fetch_pos != std::string::npos) {
    size_t offset_pos = mask.find("offset");
    std::string offset_val;
    if (offset_pos != std::string::npos && offset_pos < fetch_pos) {
      size_t cur = offset_pos + 6;
      while (cur < fetch_pos &&
             (std::isspace(static_cast<unsigned char>(lower[cur])) != 0)) {
        ++cur;
      }
      while (cur < fetch_pos &&
             (std::isdigit(static_cast<unsigned char>(lower[cur])) != 0)) {
        offset_val.push_back(lower[cur]);
        ++cur;
      }
    }

    size_t count_start = fetch_pos + 11;
    while (
        count_start < lower.size() &&
        (std::isspace(static_cast<unsigned char>(lower[count_start])) != 0)) {
      ++count_start;
    }
    std::string count_val;
    size_t count_cur = count_start;
    while (count_cur < lower.size() &&
           (std::isdigit(static_cast<unsigned char>(lower[count_cur])) != 0)) {
      count_val.push_back(lower[count_cur]);
      ++count_cur;
    }
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
  // being rewritten into invalid SQL.
  const std::string mask = ToLowerCopy(MaskLiteralsAndComments(sql));
  size_t pos = mask.find("group by distinct");
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
           (std::isspace(static_cast<unsigned char>(lower[after])) != 0)) {
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
    // Literal: run to the end of the blanked mask region; the loop stops on
    // the first char after the closing quote (the quote itself is blanked).
    ++j;
    while (j < size && mask[j] != sql[j]) {
      ++j;
    }
    if (j >= size) {
      // The literal may legitimately close on the statement's last char.
      if (j == size && sql[size - 1] == c) {
        return size;
      }
      return std::string::npos;
    }
    return j;
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

}  // namespace

GoogleSqlParseResult ParseViaSubprocess(std::string_view sql) {
  std::string current_sql =
      NormalizeCaseInsensitiveLike(NormalizeGroupByDistinct(sql));

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
      sel_line_start =
          (sel_line_start == std::string::npos) ? 0 : sel_line_start + 1;
      size_t select_indent = 0;
      while (sel_line_start + select_indent < main_res.ast.size() &&
             main_res.ast[sel_line_start + select_indent] == ' ') {
        ++select_indent;
      }

      std::string distinct_on_block =
          std::string(select_indent + 2, ' ') + "DistinctOn\n";
      size_t select_list_pos = keys_res.ast.find("SelectList");
      if (select_list_pos != std::string::npos) {
        size_t sl_line_start = keys_res.ast.rfind('\n', select_list_pos);
        sl_line_start =
            (sl_line_start == std::string::npos) ? 0 : sl_line_start + 1;
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
            if (cur_end == std::string::npos) {
              cur_end = keys_res.ast.size();
            }
            std::string line =
                keys_res.ast.substr(next_line, cur_end - next_line);
            size_t sp = 0;
            while (sp < line.size() && line[sp] == ' ') {
              ++sp;
            }
            if (sp <= sl_indent && !line.empty()) {
              break;
            }
            if (!line.empty()) {
              size_t rel = sp - sl_indent;
              distinct_on_block += std::string(select_indent + 2 + rel, ' ') +
                                   line.substr(sp) + "\n";
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
          res.ast.insert(limit_line_end + 1,
                         std::string(limit_indent + 2, ' ') + "WithTies\n");
        }
      }
    }
    return res;
  }

  return ParseRawSubprocess(current_sql);
}

#endif  // TINYLAMB_GOOGLESQL_EXECUTABLE && defined(__unix__)

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
  // reader/writer lock on OLTP parse bursts; the shard lock is deliberately
  // HELD across a cache-miss parse so a burst of distinct statements
  // serializes per shard (at most kParseCacheShards concurrent parser
  // processes) instead of forking one child per racing thread.
  const std::string cache_key(sql);
  ParseShard& shard = ShardFor(cache_key);
  std::scoped_lock cache_lock(shard.mutex);
  const auto now = std::chrono::steady_clock::now();
  {
    const auto cached = shard.cache.find(cache_key);
    if (cached != shard.cache.end()) {
      return {.ok = true, .ast = cached->second, .error = {}};
    }
    const auto failed = shard.negative_cache.find(cache_key);
    if (failed != shard.negative_cache.end()) {
      if (now - failed->second < kNegativeCacheTtl) {
        return {.ok = false,
                .ast = {},
                .error = "GoogleSQL parse failed (recently cached)"};
      }
      shard.negative_cache.erase(failed);
    }
  }

  GoogleSqlParseResult parsed = ParseViaSubprocess(sql);
  if (parsed.ok) {
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
    shard.negative_cache.insert_or_assign(cache_key,
                                          std::chrono::steady_clock::now());
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
