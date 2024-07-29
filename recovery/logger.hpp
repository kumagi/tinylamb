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

#ifndef TINYLAMB_LOGGER_HPP
#define TINYLAMB_LOGGER_HPP

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "common/constants.hpp"

namespace tinylamb {

class Logger final {
 public:
  explicit Logger(const std::filesystem::path& logfile,
                  size_t buffer_size = 1024 * 1024 * 8, size_t every_ms = 1);
  Logger(const Logger&) = delete;
  Logger(Logger&&) = delete;
  Logger& operator=(const Logger&) = delete;
  Logger& operator=(Logger&&) = delete;
  ~Logger();

  void Finish();

  [[nodiscard]] lsn_t CommittedLSN() const;

  lsn_t AddLog(std::string_view payload);

  [[nodiscard]] int Fd() const { return dst_; }

 private:
  void LoggerWork();

  // This order of member is suggested by Clang-Tidy to minimize the padding.
  std::atomic<bool> finish_ = false;
  alignas(64) std::atomic<lsn_t> flushed_lsn_{0};
  const size_t every_us_;
  std::string buffer_;
  int dst_ = -1;
  std::mutex enqueue_latch_;
  alignas(64) std::atomic<lsn_t> buffered_lsn_{0};
  std::thread worker_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOGGER_HPP
