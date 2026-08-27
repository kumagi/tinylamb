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

// NOTE: LOG() never filters levels. LOG(FATAL) flushes the message to stderr
// and then aborts the process; callers wanting a survivable diagnostic must
// use LOG(ERROR) or below.
#define LOG(level) LogMessage(level, __FILE__, __LINE__, __func__).stream()
#define STATUS(s, message)                   \
  do {                                       \
    if ((s) != Status::kSuccess) {           \
      LOG(FATAL) << (message) << ": " << (s); \
      abort();                               \
    }                                        \
  } while (0)

#ifndef ERROR_CODES_DEFINE
#define ERROR_CODES_DEFINE

enum class LogLevel : uint16_t {
  kTrace = 0,
  kDebug = 1000,
  kUser = 1500,
  kInfo = 2000,
  kNotice = 2500,
  kWarn = 3000,
  kAlert = 4000,
  kError = 5000,
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

class LogMessage;
class LogStream {
  friend class LogMessage;
  LogStream() = default;  // Only LogMessage can construct it.

 public:
  LogStream(const LogStream&) = delete;
  LogStream(LogStream&&) = delete;
  LogStream& operator=(const LogStream&) = delete;
  LogStream& operator=(LogStream&&) = delete;

  template <int N>
  LogStream& operator<<(const std::array<char, N>& rhs) {
    message_ << rhs.data();
    return *this;
  }

  template <typename T>
  LogStream& operator<<(const std::unordered_set<T>& rhs) {
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
    message_ << rhs;
    return *this;
  }

  ~LogStream();

 private:
  std::stringstream message_;
  bool fatal_{false};
};

class LogMessage {
 public:
  LogMessage(LogLevel log_level, const char* filename, int lineno,
             const char* func_name);
  LogStream& stream() { return ls; }

 private:
  LogStream ls;
};

#endif  // TINYLAMB_LOG_MESSAGE_HPP
