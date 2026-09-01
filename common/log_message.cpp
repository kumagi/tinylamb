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
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <tuple>

LogStream::~LogStream() {
  std::cerr << message_.str() << "\033[0;39;49m\n";
  if (fatal_) {
    std::abort();
  }
}

LogMessage::LogMessage(LogLevel log_level, const char* filename, int lineno,
                       const char* func_name) {
  if (log_level == LogLevel::kFatal) {
    ls.fatal_ = true;
  }
  std::array<char, 70> buff{};
  auto now = std::chrono::system_clock::now();
  std::time_t now_time = std::chrono::system_clock::to_time_t(now);
  std::tm now_tm{};
  // localtime() is not thread-safe (shared static buffer); use the reentrant
  // form so concurrent loggers on worker threads do not race.
  std::ignore = localtime_r(&now_time, &now_tm);
  std::ignore =
      strftime(buff.data(), buff.size(), "%Y-%m-%d %H:%M:%S ", &now_tm);

  switch (log_level) {
    case LogLevel::kFatal:
      ls << "\033[1;31m";
      break;
    case LogLevel::kError:
      ls << "\033[4;31m";
      break;
    case LogLevel::kAlert:
      ls << "\033[1;5;95m";
      break;
    case LogLevel::kWarn:
      ls << "\033[33m";
      break;
    case LogLevel::kNotice:
      ls << "\033[1;36m";
      break;
    case LogLevel::kInfo:
      break;  // Do nothing.
    case LogLevel::kUser:
      ls << "\033[7;32m";
      break;
    case LogLevel::kDebug:
      ls << "\033[1;34m";
      break;
    case LogLevel::kTrace:
      ls << "\033[4;36m";
      break;
    default:
      assert(!"unknown log level");
  }
  ls << buff.data() << filename << ":" << lineno << " " << func_name;
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
