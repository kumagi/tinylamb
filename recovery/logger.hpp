#ifndef TINYLAMB_LOGGER_HPP
#define TINYLAMB_LOGGER_HPP

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

#include "common/constants.hpp"

namespace tinylamb {

class Logger final {
 public:
  explicit Logger(std::string_view filename, size_t buffer_size = 1024 * 1024,
                  size_t every_ms = 1);
  Logger(const Logger&) = delete;
  Logger(Logger&&) = delete;
  Logger& operator=(const Logger&) = delete;
  Logger& operator=(Logger&&) = delete;
  ~Logger();

  void Finish();

  [[nodiscard]] lsn_t CommittedLSN() const;

  lsn_t AddLog(std::string_view payload);

 private:
  void LoggerWork();

  // This order of member is suggested by Clang-Tidy to minimize the padding.
  std::atomic<bool> finish_ = false;
  alignas(64) std::atomic<lsn_t> flushed_lsn_{0};
  const size_t every_us_;
  std::thread worker_;
  std::string buffer_;
  std::string filename_;
  std::mutex enqueue_latch_;
  alignas(64) std::atomic<lsn_t> buffered_lsn_{0};
  int dst_ = -1;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOGGER_HPP
