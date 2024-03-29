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
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define LOG(level) LogMessage(level, __FILE__, __LINE__, __func__).stream()
#define STATUS(s, message)                  \
  {                                         \
    if ((s) != Status::kSuccess) {          \
      LOG(FATAL) << (message) << ": " << s; \
      abort();                              \
    }                                       \
  }

#ifndef ERROR_CODES_DEFINE
#define ERROR_CODES_DEFINE
#define FATAL 9000
#define ERROR 5000
#define ALERT 4000
#define WARN 3000
#define NOTICE 2500
#define INFO 2000
#define USER 1500
#define DEBUG 1000
#define TRACE 0
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
};

class LogMessage {
 public:
  LogMessage(int log_level, const char* filename, int lineno,
             const char* func_name);
  LogStream& stream() { return ls; }

 private:
  LogStream ls;
};

#endif  // TINYLAMB_LOG_MESSAGE_HPP
