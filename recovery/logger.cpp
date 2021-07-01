
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
  log.SetLSN(next_lsn_++);
  std::string data = log.Serialize();
  for (;;) {
    std::scoped_lock lk(latch_);
    const uint64_t lsn = next_lsn_;
    if (buffer_.size() < written_pos_ + data.size()) {
      const size_t fraction = buffer_.size() - written_pos_;
      std::memcpy(buffer_.data() + written_pos_, data.data(), fraction);
      std::memcpy(buffer_.data(), data.data() + fraction,
                  data.size() - fraction);
      written_pos_ = (written_pos_ + data.size()) % buffer_.size();
    } else {
      memcpy(buffer_.data() + written_pos_, data.data(), data.size());
      written_pos_ += data.size();
    }
    lsn_entry_.emplace_back(lsn, written_pos_);
    if (buffer_.size() - RestSize() < buffer_.size() / 4) {
      worker_wait_.notify_all();
    }
    return lsn;
  }
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
  while (!finish_) {
    worker_wait_.wait_for(lk, std::chrono::milliseconds(every_ms_));
    if (flushed_pos_ == written_pos_) continue;
    if (flushed_pos_ < written_pos_) {
      assert(written_pos_ < buffer_.size());
      size_t flushed_bytes = write(dst_, buffer_.data() + flushed_pos_,
                                   written_pos_ - flushed_pos_);
      ::fsync(dst_);
      flushed_pos_ += flushed_bytes;
      while (!lsn_entry_.empty() &&
             lsn_entry_.front().position <= flushed_pos_) {
        committed_lsn_ = lsn_entry_.front().lsn;
        lsn_entry_.pop_front();
      }
    } else {
      size_t flushed_bytes = write(dst_, buffer_.data() + flushed_pos_,
                                   buffer_.size() - flushed_pos_);
      if (0 < flushed_bytes) {
        flushed_bytes += write(dst_, buffer_.data(), written_pos_);
      }
      ::fsync(dst_);
      flushed_pos_ = (flushed_pos_ + flushed_bytes) % buffer_.size();
      while (!lsn_entry_.empty() &&
             lsn_entry_.front().position <= buffer_.size()) {
        committed_lsn_ = lsn_entry_.front().lsn;
        lsn_entry_.pop_front();
      }
      while (!lsn_entry_.empty() &&
             lsn_entry_.front().position <= flushed_pos_) {
        committed_lsn_ = lsn_entry_.front().lsn;
        lsn_entry_.pop_front();
      }
    }
    fsync(dst_);
  }
}

[[nodiscard]] size_t Logger::RestSize() const {
  if (flushed_pos_ <= written_pos_)
    return buffer_.size() - written_pos_ + flushed_pos_ - 1;
  else
    return flushed_pos_ - written_pos_ - 1;
}

}  // namespace tinylamb
