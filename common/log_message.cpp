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
#include <ctime>
#include <iostream>
#include <tuple>

LogStream::~LogStream() { std::cerr << message_.str() << "\e[0;39;49m\n"; }

LogMessage::LogMessage(int log_level, const char* filename, int lineno,
                       const char* func_name) {
  std::array<char, 70> buff{};
  auto now = std::chrono::system_clock::now();
  std::time_t now_time = std::chrono::system_clock::to_time_t(now);
  std::tm now_tm = *std::localtime(&now_time);
  std::ignore =
      strftime(buff.data(), buff.size(), "%Y-%m-%d %H:%M:%S ", &now_tm);

  switch (log_level) {
    case FATAL:
      ls << "\e[1;31m";
      break;
    case ERROR:
      ls << "\e[4;31m";
      break;
    case ALERT:
      ls << "\e[1;5;95m";
      break;
    case WARN:
      ls << "\e[33m";
      break;
    case NOTICE:
      ls << "\e[1;36m";
      break;
    case INFO:
      break;  // Do nothing.
    case USER:
      ls << "\e[7;32m";
      break;
    case DEBUG:
      ls << "\e[1;34m";
      break;
    case TRACE:
      ls << "\e[4;36m";
      break;
    default:
      assert(!"unknown log level");
  }
  ls << buff.data() << filename << ":" << lineno << " " << func_name;
  switch (log_level) {
    case FATAL:
      ls << " FATAL  ";
      break;
    case ERROR:
      ls << " ERROR  ";
      break;
    case ALERT:
      ls << " ALERT  ";
      break;
    case WARN:
      ls << " WARN   ";
      break;
    case NOTICE:
      ls << " NOTICE ";
      break;
    case INFO:
      ls << " INFO   ";
      break;
    case USER:
      ls << " USER   ";
      break;
    case DEBUG:
      ls << " DEBUG  ";
      break;
    case TRACE:
      ls << " TRACE  ";
      break;
    default:
      ls << "UNKNOWN LOG LEVEL ";
  }
  ls << " - ";
}
