
#include "recovery/logger.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <mutex>

#include "log_record.hpp"

namespace tinylamb {

Logger::Logger(std::string_view filename, size_t buffer_size, size_t every_ms)
    : every_us_(every_ms),
      worker_(&Logger::LoggerWork, this),
      buffer_(buffer_size, 0),
      filename_(filename),
      dst_(open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666)) {
  memset(buffer_.data(), 0, buffer_size);
}

Logger::~Logger() {
  Finish();
  close(dst_);
}

void Logger::Finish() {
  finish_ = true;
  worker_.join();
}

lsn_t Logger::CommittedLSN() const {
  return flushed_lsn_.load(std::memory_order_release);
}

lsn_t Logger::AddLog(std::string_view payload) {
  std::unique_lock enq_lk{enqueue_latch_};
  const lsn_t lsn = buffered_lsn_.load(std::memory_order_relaxed);
  while (!payload.empty()) {
    const size_t buffered_lsn = buffered_lsn_.load(std::memory_order_seq_cst);
    const size_t flushed_lsn = flushed_lsn_.load(std::memory_order_seq_cst);

    if (buffered_lsn - flushed_lsn == buffer_.size()) {
      // Has no space in the buffer, wait for worker.
      std::this_thread::sleep_for(std::chrono::microseconds(1));
      continue;
    }
    const size_t buffered = buffered_lsn % buffer_.size();
    const size_t flushed = flushed_lsn % buffer_.size();
    const size_t write_size =
        flushed <= buffered
            ? std::min(payload.size(), buffer_.size() - buffered)
            : std::min(payload.size(), flushed - buffered);
    memcpy(buffer_.data() + buffered, payload.data(), write_size);

    // Forward buffer pointer.
    buffered_lsn_.store(buffered_lsn + write_size, std::memory_order_release);

    payload.remove_prefix(write_size);
  }
  return lsn;
}

void Logger::LoggerWork() {
  while (!finish_ || flushed_lsn_ < buffered_lsn_) {
    const size_t flushed_lsn = flushed_lsn_.load(std::memory_order_relaxed);
    const size_t buffered_lsn = buffered_lsn_.load(std::memory_order_acquire);

    if (flushed_lsn == buffered_lsn) {
      // No data to flush.
      std::this_thread::sleep_for(std::chrono::microseconds(every_us_));
      continue;
    }

    const size_t buffered = buffered_lsn % buffer_.size();
    const size_t flushed = flushed_lsn % buffer_.size();

    const ssize_t flushed_size =
        write(dst_, buffer_.data() + flushed,
              (flushed < buffered ? buffered : buffer_.size()) - flushed);
    if (flushed_size <= 0) {
      LOG(FATAL) << "Failed to flush.";
      continue;
    }

    fdatasync(dst_);  // Flush!
    flushed_lsn_.store(flushed_lsn + flushed_size, std::memory_order_release);
  }
}

}  // namespace tinylamb
