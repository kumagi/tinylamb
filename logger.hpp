#ifndef TINYLAMB_LOGGER_HPP
#define TINYLAMB_LOGGER_HPP

#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>

class Logger {
 public:
  Logger(std::string_view filename, size_t buffer_size = 1024 * 1024,
         size_t every_ms = 20)
      : filename_(filename),
        dst_(open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666)),
        worker_(LoggerWork, this),
        buffer_(buffer_size, 0),
        every_ms_(every_ms) {
  }

  ~Logger() {
    close(dst_);
  }

  bool AddLog(uint64_t lsn, std::string_view log) {
    std::scoped_lock lk(latch_);
    if (RestSize() < log.size()) return;
    if (buffer_.size() < written_pos_ + log.size()) {
      const size_t fraction = buffer_.size() - written_pos_;
      memcpy(buffer_.data() + written_pos_, log.data(), fraction);
      memcpy(buffer_.data(), log.data() + fraction, log.size() - fraction);
      written_pos_ = (written_pos_ + log.size()) % buffer_.size();
    } else {
      memcpy(buffer_.data() + written_pos_, log.data(), log.size());
    }
  }

  void Finish() {
    finish_ = true;
    worker_.join();
  }

 private:
  void LoggerWork() {
    std::scoped_lock lk(latch_);
    while (!finish_) {
      worker_wait_.wait_for(latch_, std::chrono::milliseconds(every_ms_));
      if (flushed_pos_ == written_pos_) continue;
      if (flushed_pos_ < written_pos_) {
        write(dst_, buffer_.data() + flushed_pos_, written_pos_ - flushed_pos_);
      } else {
        write(dst_, buffer_.data() + flushed_pos_, buffer_.size() - flushed_pos_);
        write(dst_, buffer_.data(), flushed_pos_, dst_);
      }
      fsync(dst_);
    }
  }

  [[nodiscard]] size_t RestSize() const {
    if (flushed_pos_ < written_pos_)
      return buffer_.size() + written_pos_ - flushed_pos_;
    else
      return written_pos_ - flushed_pos_;
  }

 private:
  std::string filename_;
  int dst_ = nullptr;
  std::thread worker_;
  std::condition_variable worker_wait_;
  std::atomic<bool> finish_ = false;
  std::string buffer_;
  std::mutex latch_;
  const size_t every_ms_;
  uint64_t committed_lsn_;
  size_t written_pos_;
  size_t flushed_pos_;
};

#endif  // TINYLAMB_LOGGER_HPP
