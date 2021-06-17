#ifndef TINYLAMB_LOGGER_HPP
#define TINYLAMB_LOGGER_HPP

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

namespace tinylamb {

class Logger {
  struct LsnIndex {
    uint64_t lsn;
    size_t position;
    LsnIndex(uint64_t l, size_t p) : lsn(l), position(p) {}
  };

 public:
  Logger(std::string_view filename, size_t buffer_size = 1024 * 1024,
         size_t every_ms = 20)
      : filename_(filename),
        dst_(open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666)),
        worker_(&Logger::LoggerWork, this),
        buffer_(buffer_size, 0),
        every_ms_(every_ms) {}

  ~Logger() {
    Finish();
    close(dst_);
  }

  bool AddLog(uint64_t lsn, const void* buffer, size_t length) {
    return AddLog(lsn,std::string_view(reinterpret_cast<const char*>(buffer),
                                       length));
  }

  bool AddLog(uint64_t lsn, std::string_view log) {
    std::scoped_lock lk(latch_);
    if (lsn <= committed_lsn_ || RestSize() < log.size()) return false;
    if (buffer_.size() < written_pos_ + log.size()) {
      const size_t fraction = buffer_.size() - written_pos_;
      memcpy(buffer_.data() + written_pos_, log.data(), fraction);
      memcpy(buffer_.data(), log.data() + fraction, log.size() - fraction);
      written_pos_ = (written_pos_ + log.size()) % buffer_.size();
    } else {
      memcpy(buffer_.data() + written_pos_, log.data(), log.size());
      written_pos_ += log.size();
    }
    lsn_entry_.emplace_back(lsn, written_pos_);
    if (512 < RestSize() - buffer_.size()) {
      worker_wait_.notify_all();
    }
    return true;
  }

  void Finish() {
    finish_ = true;
    worker_.join();
  }

  uint64_t CommittedLSN() const {
    std::scoped_lock lk(latch_);
    return committed_lsn_;
  }

 private:
  void LoggerWork() {
    std::unique_lock lk(latch_);
    while (!finish_) {
      worker_wait_.wait_for(lk, std::chrono::milliseconds(every_ms_));
      if (flushed_pos_ == written_pos_) continue;
      if (flushed_pos_ < written_pos_) {
        size_t flushed_bytes = write(dst_, buffer_.data() + flushed_pos_,
                                     written_pos_ - flushed_pos_);
        fsync(dst_);
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
        fsync(dst_);
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

  [[nodiscard]] size_t RestSize() const {
    if (flushed_pos_ <= written_pos_)
      return buffer_.size() - written_pos_ + flushed_pos_ - 1;
    else
      return flushed_pos_ - written_pos_ - 1;
  }

 private:
  std::string filename_;
  int dst_ = -1;
  std::thread worker_;
  std::condition_variable worker_wait_;
  std::atomic<bool> finish_ = false;
  std::string buffer_;
  mutable std::mutex latch_;

  const size_t every_ms_;
  uint64_t committed_lsn_ = 0;
  size_t written_pos_ = 0;
  size_t flushed_pos_ = 0;
  std::deque<LsnIndex> lsn_entry_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOGGER_HPP
