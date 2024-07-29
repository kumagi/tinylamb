//
// Created by kumagi on 24/07/21.
//

#ifndef TINYLAMB_RING_BUFFER_HPP
#define TINYLAMB_RING_BUFFER_HPP

#include <assert.h>

#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <valarray>
#include <vector>

namespace tinylamb {

namespace {
inline int NearestPowerOf2(int n) {
  if (n == 0) {
    return 1;
  }
  int a = std::log(n);
  if (std::pow(2, a) == n) {
    return n;
  }
  return pow(2, a + 1);
}
}  // namespace

// Thread UN-safe ring buffer.
template <typename T>
class SimpleRingBuffer {
 public:
  explicit SimpleRingBuffer(size_t size) : buffer_(NearestPowerOf2(size)) {
    assert(std::popcount(buffer_.size()) == 1);
  }

  // Returns true on success. Fails if the buffer is empty.
  bool Enqueue(T item) {
    if (write_idx_ - read_idx_ == buffer_.size()) {
      return false;
    }
    buffer_[write_idx_ & (buffer_.size() - 1)] = item;
    write_idx_++;
    return true;
  }

  // Returns true on success. Fails if the buffer is full.
  bool Dequeue(T* dest) {
    if (write_idx_ == read_idx_) {
      return false;
    }
    *dest = buffer_[read_idx_ & (buffer_.size() - 1)];
    read_idx_++;
    return true;
  }

  [[nodiscard]] bool IsEmpty() const { return read_idx_ == write_idx_; }
  [[nodiscard]] bool IsFull() const {
    return write_idx_ - read_idx_ == buffer_.size();
  }
  [[nodiscard]] size_t Capacity() const { return buffer_.size(); }

 private:
  std::vector<T> buffer_;
  uint64_t read_idx_{0};
  uint64_t write_idx_{0};
};

// Thread safe single-producer / single-consumer ring buffer.
template <typename T>
class RingBuffer {
  static_assert(static_cast<bool>(std::is_trivially_copyable_v<T>));

 public:
  explicit RingBuffer(size_t size = 256) : buffer_(NearestPowerOf2(size)) {
    assert(std::popcount(buffer_.size()) == 1);
  }
  RingBuffer(const RingBuffer&) = delete;
  RingBuffer(RingBuffer&&) = delete;
  RingBuffer& operator=(const RingBuffer&) = delete;
  RingBuffer& operator=(RingBuffer&&) = delete;
  ~RingBuffer() = default;

  // Returns true on success. Fails if the buffer is empty.
  bool Enqueue(T item) {
    uint64_t write_idx = write_idx_.load(std::memory_order_relaxed);
    if (write_idx - cached_read_idx_ == buffer_.size()) {
      cached_read_idx_ = read_idx_.load(std::memory_order_acquire);
      assert(cached_read_idx_ <= write_idx);
      if (write_idx - cached_read_idx_ == buffer_.size()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return false;
      }
    }
    buffer_[write_idx & (buffer_.size() - 1)] = item;
    write_idx_.store(write_idx + 1, std::memory_order_release);
    return true;
  }

  // Returns true on success. Fails if the buffer is full.
  bool Dequeue(T* dest) {
    uint64_t read_idx = read_idx_.load(std::memory_order_release);
    if (cached_write_idx_ == read_idx) {
      cached_write_idx_ = write_idx_.load(std::memory_order_acquire);
      assert(read_idx <= cached_write_idx_);
      if (cached_write_idx_ == read_idx) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return false;
      }
    }
    *dest = buffer_[read_idx & (buffer_.size() - 1)];
    read_idx_.store(read_idx + 1, std::memory_order_release);
    return true;
  }

  [[nodiscard]] bool IsEmpty() const { return read_idx_ == write_idx_; }
  [[nodiscard]] bool IsFull() const {
    return write_idx_ - read_idx_ == buffer_.size();
  }
  [[nodiscard]] size_t Capacity() const { return buffer_.size(); }

 private:
  std::vector<T> buffer_;
  alignas(64) std::atomic<uint64_t> read_idx_{0};
  alignas(64) uint64_t cached_read_idx_{0};
  alignas(64) std::atomic<uint64_t> write_idx_{0};
  alignas(64) uint64_t cached_write_idx_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_RING_BUFFER_HPP
