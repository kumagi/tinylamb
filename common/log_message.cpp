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

#include "log_message.hpp"

#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <mutex>
#include <string_view>
#include <tuple>

#ifndef _WIN32
#include <unistd.h>
#endif

#include "common/platform/synced_stream.hpp"
#include "common/platform/time_format.hpp"

namespace tinylamb::detail {
[[noreturn]] void LogFatalAbort() { std::abort(); }

namespace {
// One relaxed atomic is enough: a stale read only means a few extra messages
// around a level change at process startup.
std::atomic<int> g_min_log_level{static_cast<int>(LogLevel::kTrace)};
std::once_flag g_log_level_env_once;

LogLevel ParseLogLevelEnv() {
  const char* env = std::getenv("TINYLAMB_LOG_LEVEL");
  if (env == nullptr || *env == '\0') {
    return LogLevel::kTrace;
  }
  const std::string value = [env] {
    std::string s(env);
    for (char& c : s) {
      c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return s;
  }();
  if (value == "TRACE") return LogLevel::kTrace;  // NOLINT(readability-braces-around-statements)
  if (value == "DEBUG") return LogLevel::kDebug;  // NOLINT(readability-braces-around-statements)
  if (value == "USER") return LogLevel::kUser;  // NOLINT(readability-braces-around-statements)
  if (value == "INFO") return LogLevel::kInfo;  // NOLINT(readability-braces-around-statements)
  if (value == "NOTICE") return LogLevel::kNotice;  // NOLINT(readability-braces-around-statements)
  if (value == "WARN" || value == "WARNING") return LogLevel::kWarn;  // NOLINT(readability-braces-around-statements)
  if (value == "ERROR") return LogLevel::kError;  // NOLINT(readability-braces-around-statements)
  if (value == "ALERT") return LogLevel::kAlert;  // NOLINT(readability-braces-around-statements)
  if (value == "FATAL") return LogLevel::kFatal;  // NOLINT(readability-braces-around-statements)
  return LogLevel::kTrace;
}

void EnsureLogLevelInitialized() noexcept {
  std::call_once(g_log_level_env_once, [] {
    g_min_log_level.store(static_cast<int>(ParseLogLevelEnv()),
                           std::memory_order_relaxed);
  });
}
}  // namespace

void SetMinLogLevel(LogLevel level) noexcept {
  EnsureLogLevelInitialized();
  g_min_log_level.store(static_cast<int>(level), std::memory_order_relaxed);
}

LogLevel MinLogLevel() noexcept {
  EnsureLogLevelInitialized();
  return static_cast<LogLevel>(g_min_log_level.load(std::memory_order_relaxed));
}

bool LogLevelEnabled(LogLevel level) noexcept {
  EnsureLogLevelInitialized();
  const int min = g_min_log_level.load(std::memory_order_relaxed);
  return level == LogLevel::kFatal || static_cast<int>(level) >= min;
}

}  // namespace tinylamb::detail

namespace {
bool LogUsesColor() {
  static const bool kUseColor = [] {
    if (std::getenv("NO_COLOR") != nullptr) {
      return false;
    }
    if (std::getenv("TINYLAMB_FORCE_COLOR") != nullptr) {
      return true;
    }
#ifndef _WIN32
    return isatty(2) != 0;
#else
    return false;
#endif
  }();
  return kUseColor;
}
}  // namespace

LogStream::~LogStream() {
  if (enabled_) {
    tinylamb::platform::SyncedOStream(std::cerr)
        << message_.str() << (LogUsesColor() ? "\033[0;39;49m" : "") << "\n";
  }
  if (fatal_) {
    std::abort();
  }
}

LogMessage::LogMessage(LogLevel log_level, const char* filename, int lineno,
                       const char* func_name, bool enabled) {
  ls.enabled_ = enabled;
  if (!enabled) {
    // Filtered message: skip the clock read, localtime_r, strftime and all
    // prefix formatting; operator<< and the destructor are no-ops too.
    return;
  }
  if (log_level == LogLevel::kFatal) {
    ls.fatal_ = true;
  }
  std::array<char, 70> buff{};
  auto now = std::chrono::system_clock::now();
  std::time_t now_time = std::chrono::system_clock::to_time_t(now);
  std::tm now_tm{};
  // localtime() is not thread-safe (shared static buffer); use the reentrant
  // form so concurrent loggers on worker threads do not race.
  const bool local_ok =
      tinylamb::platform::LocalTime(now_time, &now_tm);
  const size_t formatted =
      local_ok ? std::strftime(buff.data(), buff.size(),
                              "%Y-%m-%d %H:%M:%S ", &now_tm)
               : 0;
  const std::string_view timestamp =
      formatted == 0 ? std::string_view("<time?> ")
                     : std::string_view(buff.data(), formatted);

  switch (log_level) {
    case LogLevel::kFatal:
      if (LogUsesColor()) {
        ls << "\033[1;31m";
      }
      break;
    case LogLevel::kError:
      if (LogUsesColor()) {
        ls << "\033[4;31m";
      }
      break;
    case LogLevel::kAlert:
      if (LogUsesColor()) {
        ls << "\033[1;5;95m";
      }
      break;
    case LogLevel::kWarn:
      if (LogUsesColor()) {
        ls << "\033[33m";
      }
      break;
    case LogLevel::kNotice:
      if (LogUsesColor()) {
        ls << "\033[1;36m";
      }
      break;
    case LogLevel::kInfo:
      break;  // Do nothing.
    case LogLevel::kUser:
      if (LogUsesColor()) {
        ls << "\033[7;32m";
      }
      break;
    case LogLevel::kDebug:
      if (LogUsesColor()) {
        ls << "\033[1;34m";
      }
      break;
    case LogLevel::kTrace:
      if (LogUsesColor()) {
        ls << "\033[4;36m";
      }
      break;
    default:
      break;
  }
  ls << timestamp << filename << ":" << lineno << " " << func_name;
  switch (log_level) {
    case LogLevel::kFatal:
      ls << " FATAL  ";
      break;
    case LogLevel::kError:
      ls << " ERROR  ";
      break;
    case LogLevel::kAlert:
      ls << " ALERT  ";
      break;
    case LogLevel::kWarn:
      ls << " WARN   ";
      break;
    case LogLevel::kNotice:
      ls << " NOTICE ";
      break;
    case LogLevel::kInfo:
      ls << " INFO   ";
      break;
    case LogLevel::kUser:
      ls << " USER   ";
      break;
    case LogLevel::kDebug:
      ls << " DEBUG  ";
      break;
    case LogLevel::kTrace:
      ls << " TRACE  ";
      break;
    default:
      ls << "UNKNOWN LOG LEVEL ";
  }
  ls << " - ";
}
