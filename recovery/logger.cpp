
#include "recovery/logger.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
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

uint64_t Logger::AddLog(LogRecord& log) {
  std::string data = log.Serialize();
  std::scoped_lock lk(latch_);

  size_t memory_offset = written_lsn_ % buffer_.size();
  if (buffer_.size() < memory_offset + data.size()) {
    const size_t fraction = buffer_.size() - memory_offset;
    std::memcpy(buffer_.data() + memory_offset, data.data(), fraction);
    std::memcpy(buffer_.data(), data.data() + fraction, data.size() - fraction);
  } else {
    memcpy(buffer_.data() + memory_offset, data.data(), data.size());
  }
  written_lsn_ += data.size();
  if (buffer_.size() - RestSize() < buffer_.size() / 4) {
    worker_wait_.notify_all();
  }
  return written_lsn_ - data.size();
}

void Logger::Finish() {
  finish_ = true;
  worker_.join();
}

uint64_t Logger::CommittedLSN() const {
  std::scoped_lock lk(latch_);
  return committed_lsn_;
}

void Logger::LoggerWork() {
  std::unique_lock lk(latch_);
  while (!finish_ || committed_lsn_ != written_lsn_) {
    worker_wait_.wait_for(lk, std::chrono::milliseconds(every_ms_));
    if (committed_lsn_ == written_lsn_) continue;
    const size_t written_offset = written_lsn_ % buffer_.size();
    const size_t committed_offset = committed_lsn_ % buffer_.size();
    size_t flushed_bytes = 0;
    if (committed_lsn_ < written_lsn_) {
      flushed_bytes += write(dst_, buffer_.data() + committed_offset,
                             written_offset - committed_offset);
    } else {
      flushed_bytes += write(dst_, buffer_.data() + committed_offset,
                             buffer_.size() - committed_offset);
      if (0 < flushed_bytes) {
        flushed_bytes += write(dst_, buffer_.data(), written_offset);
      }
    }
    committed_lsn_ += flushed_bytes;
    fsync(dst_);
  }
}

[[nodiscard]] size_t Logger::RestSize() const {
  if (committed_lsn_ <= written_lsn_)
    return buffer_.size() - written_lsn_ + committed_lsn_ - 1;
  else
    return committed_lsn_ - written_lsn_ - 1;
}

}  // namespace tinylamb
