/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_LOG_MESSAGE_HPP
#define TINYLAMB_LOG_MESSAGE_HPP

#include <array>
#include <cstdint>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// LOG() filters by the process minimum level (see TINYLAMB_LOG_LEVEL). A
// filtered message is a no-op: no timestamp, no stringstream formatting and
// no cerr lock. LOG(FATAL) is never filtered -- it flushes to stderr and then
// aborts; callers wanting a survivable diagnostic must use LOG(ERROR) or below.
#define LOG(level)                                                         \
  LogMessage(level, __FILE__, __LINE__, __func__,                            \
             ::tinylamb::detail::LogLevelEnabled(level))                    \
      .stream()
#define STATUS(s, message)                    \
  do {                                        \
    if ((s) != Status::kSuccess) {            \
      LOG(FATAL) << (message) << ": " << (s); \
      abort();                                \
    }                                         \
  } while (0)

// Google-style assertion for invariant violations (programmer errors):
// logs FATAL and aborts instead of throwing. DB logic propagates runtime
// errors as Status/StatusOr; only "this branch must be unreachable" and
// "the caller violated a precondition" belong here.
#define CHECK(condition)                            \
  do {                                              \
    if (!(condition)) {                             \
      LOG(FATAL) << "CHECK failed: " << #condition; \
      ::tinylamb::detail::LogFatalAbort();          \
    }                                               \
  } while (0)

#define CHECK_MSG(condition, message)              \
  do {                                             \
    if (!(condition)) {                            \
      LOG(FATAL) << "CHECK failed: " << (message); \
      ::tinylamb::detail::LogFatalAbort();         \
    }                                              \
  } while (0)

namespace tinylamb::detail {
[[noreturn]] void LogFatalAbort();
}  // namespace tinylamb::detail

#ifndef ERROR_CODES_DEFINE
#define ERROR_CODES_DEFINE

enum class LogLevel : uint16_t {
  kTrace = 0,
  kDebug = 1000,
  kUser = 1500,
  kInfo = 2000,
  kNotice = 2500,
  kWarn = 3000,
  kError = 4000,
  kAlert = 5000,  // more severe than ERROR (syslog ALERT < ERR is inverted)
  kFatal = 9000,
};

#define FATAL LogLevel::kFatal
#define ERROR LogLevel::kError
#define ALERT LogLevel::kAlert
#define WARN LogLevel::kWarn
#define NOTICE LogLevel::kNotice
#define INFO LogLevel::kInfo
#define USER LogLevel::kUser
#define DEBUG LogLevel::kDebug
#define TRACE LogLevel::kTrace
#endif  // ERROR_CODE_DEFINE

namespace tinylamb::detail {
// The minimum level that is emitted. Set from TINYLAMB_LOG_LEVEL at startup;
// defaults to kTrace (no filtering) so observable behaviour is unchanged
// unless a deployment opts in. FATAL always emits regardless of the threshold.
void SetMinLogLevel(LogLevel level) noexcept;
[[nodiscard]] LogLevel MinLogLevel() noexcept;
[[nodiscard]] bool LogLevelEnabled(LogLevel level) noexcept;
}  // namespace tinylamb::detail

class LogMessage;
class LogStream {
  friend class LogMessage;
  LogStream() = default;  // Only LogMessage can construct it.

 public:
  LogStream(const LogStream&) = delete;
  LogStream(LogStream&&) = delete;
  LogStream& operator=(const LogStream&) = delete;
  LogStream& operator=(LogStream&&) = delete;

  template <size_t N>
  LogStream& operator<<(const std::array<char, N>& rhs) {
    if (!enabled_) return *this;  // NOLINT(readability-braces-around-statements)
    // Route through string_view with the explicit array extent: streaming the
    // raw array pointer would make the ostream call strlen(), reading past the
    // array when it is not NUL-terminated and truncating embedded NUL bytes.
    message_ << std::string_view(rhs.data(), N);
    return *this;
  }

  template <typename T>
  LogStream& operator<<(const std::unordered_set<T>& rhs) {
    if (!enabled_) return *this;  // NOLINT(readability-braces-around-statements)
    message_ << "{";
    bool first = true;
    for (const auto& r : rhs) {
      if (!first) {
        message_ << ", ";
      }
      message_ << r;
      first = false;
    }
    message_ << "}";
    return *this;
  }

  template <typename K, typename V>
  LogStream& operator<<(const std::unordered_map<K, V>& rhs) {
    if (!enabled_) return *this;  // NOLINT(readability-braces-around-statements)
    message_ << "{";
    bool first = true;
    for (const auto& r : rhs) {
      if (!first) {
        message_ << ", ";
      }
      message_ << r.first << " => " << r.second;
      first = false;
    }
    message_ << "}";
    return *this;
  }

  template <typename T>
  LogStream& operator<<(const std::vector<T>& rhs) {
    if (!enabled_) return *this;  // NOLINT(readability-braces-around-statements)
    message_ << "[";
    bool first = true;
    for (const auto& r : rhs) {
      if (!first) {
        message_ << ", ";
      }
      message_ << r;
      first = false;
    }
    message_ << "]";
    return *this;
  }

  template <typename T>
  LogStream& operator<<(const T& rhs) {
    if (!enabled_) return *this;  // NOLINT(readability-braces-around-statements)
    message_ << rhs;
    return *this;
  }

  ~LogStream();

 private:
  friend class LogMessage;
  std::stringstream message_;
  bool fatal_{false};
  bool enabled_{true};
};

class LogMessage {
 public:
  LogMessage(LogLevel log_level, const char* filename, int lineno,
             const char* func_name, bool enabled);
  LogStream& stream() { return ls; }

 private:
  LogStream ls;
};

#endif  // TINYLAMB_LOG_MESSAGE_HPP
