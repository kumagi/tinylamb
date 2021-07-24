#ifndef TINYLAMB_LOGGER_HPP
#define TINYLAMB_LOGGER_HPP

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <cassert>
#include <deque>
#include <mutex>
#include <thread>

namespace tinylamb {

struct LogRecord;

class Logger {
  struct LsnIndex {
    uint64_t lsn;
    size_t position;
    LsnIndex(uint64_t l, size_t p) : lsn(l), position(p) {}
  };

 public:
  Logger(std::string_view filename, size_t buffer_size = 1024 * 1024,
         size_t every_ms = 20);

  ~Logger();

  uint64_t AddLog(LogRecord& log);

  void Finish();

  uint64_t CommittedLSN() const;

 private:
  void LoggerWork();

  [[nodiscard]] size_t RestSize() const;

 private:
  mutable std::mutex latch_;
  std::atomic<uint64_t> next_lsn_ = 1;
  std::string buffer_;
  std::string filename_;
  int dst_ = -1;
  std::atomic<bool> finish_ = false;

  const size_t every_ms_;
  uint64_t committed_lsn_ = 0;
  std::atomic<size_t> written_pos_ = 0;
  std::atomic<size_t> flushed_pos_ = 0;
  std::deque<LsnIndex> lsn_entry_;

  std::condition_variable worker_wait_;
  std::thread worker_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOGGER_HPP