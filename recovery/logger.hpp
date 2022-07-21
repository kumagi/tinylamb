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

struct LogRecord;

class Logger {
 public:
  explicit Logger(std::string_view filename, size_t buffer_size = 1024 * 1024,
                  size_t every_ms = 20);

  ~Logger();

  lsn_t AddLog(std::string_view log);

  void Finish();

  lsn_t CommittedLSN() const;

 private:
  void LoggerWork();

  [[nodiscard]] size_t RestSize() const;

 private:
  mutable std::mutex latch_;
  std::string buffer_;
  std::string filename_;
  int dst_ = -1;
  std::atomic<bool> finish_ = false;

  const size_t every_ms_;
  std::atomic<lsn_t> queued_lsn_{0};
  std::atomic<lsn_t> persistent_lsn_{0};

  std::condition_variable worker_wait_;
  std::thread worker_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOGGER_HPP
