
#include "recovery/logger.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <mutex>

#include "log_record.hpp"

namespace tinylamb {

Logger::Logger(std::string_view filename, size_t buffer_size, size_t every_ms)
    : buffer_(buffer_size, 0),
      filename_(filename),
      dst_(open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666)),
      every_ms_(every_ms),
      worker_(&Logger::LoggerWork, this) {
  memset(buffer_.data(), 0, buffer_size);
}

Logger::~Logger() {
  Finish();
  close(dst_);
}

lsn_t Logger::AddLog(const LogRecord& log) {
  std::string data = log.Serialize();
  std::unique_lock lk{latch_};

  while (buffer_.size() - data.size() <= queued_lsn_ - persistent_lsn_) {
    // Has no enough space in buffer, wait for worker.
    worker_wait_.notify_all();
    worker_wait_.wait_for(lk, std::chrono::milliseconds(1));
  }

  size_t memory_offset = queued_lsn_ % buffer_.size();
  if (memory_offset + data.size() <= buffer_.size()) {
    memcpy(buffer_.data() + memory_offset, data.data(), data.size());
  } else {
    // In case of buffer wrap-around.
    const size_t fraction = buffer_.size() - memory_offset;
    std::memcpy(buffer_.data() + memory_offset, data.data(), fraction);
    std::memcpy(buffer_.data(), data.data() + fraction, data.size() - fraction);
  }

  // Move buffer pointer forward
  queued_lsn_ += data.size();
  if (buffer_.size() - RestSize() < buffer_.size() / 4) {
    worker_wait_.notify_all();
  }
  return queued_lsn_.load() - data.size();
}

void Logger::Finish() {
  finish_ = true;
  worker_.join();
}

lsn_t Logger::CommittedLSN() const {
  std::scoped_lock lk(latch_);
  return persistent_lsn_.load(std::memory_order_release);
}

void Logger::LoggerWork() {
  std::unique_lock lk(latch_);
  while (!finish_ || persistent_lsn_ != queued_lsn_) {
    worker_wait_.wait_for(lk, std::chrono::milliseconds(every_ms_));
    if (persistent_lsn_ == queued_lsn_) {
      continue;
    }

    // Write buffered data between committed_offset to written_offset.
    const size_t written_offset = queued_lsn_ % buffer_.size();
    const size_t committed_offset = persistent_lsn_ % buffer_.size();

    ssize_t flushed_bytes = 0;
    if (committed_offset < written_offset) {
      flushed_bytes += write(dst_, buffer_.data() + committed_offset,
                             written_offset - committed_offset);
      if (flushed_bytes <= 0) {
        continue;
      }
    } else {
      // In case of buffer does wrap around.
      flushed_bytes += write(dst_, buffer_.data() + committed_offset,
                             buffer_.size() - committed_offset);
      if (flushed_bytes <= 0) {
        continue;
      }
      if (static_cast<size_t>(flushed_bytes) ==
          buffer_.size() - committed_offset) {
        ssize_t extra_wrote = write(dst_, buffer_.data(), written_offset);
        if (0 < extra_wrote) {
          flushed_bytes += extra_wrote;
        }
      }
    }

    fdatasync(dst_);
    persistent_lsn_ += flushed_bytes;
  }
}

[[nodiscard]] size_t Logger::RestSize() const {
  if (persistent_lsn_ <= queued_lsn_) {
    return buffer_.size() - queued_lsn_ + persistent_lsn_ - 1;
  }
  return persistent_lsn_ - queued_lsn_ - 1;
}

}  // namespace tinylamb
