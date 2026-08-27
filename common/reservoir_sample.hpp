/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_RESERVOIR_SAMPLE_HPP
#define TINYLAMB_RESERVOIR_SAMPLE_HPP

#include <cstddef>
#include <random>
#include <utility>
#include <vector>

namespace tinylamb {

template <typename T>
class ReservoirSample {
 public:
  explicit ReservoirSample(size_t capacity, uint64_t seed = 42)
      : capacity_(capacity), rng_(seed) {}

  void Add(T item) {
    ++seen_count_;
    if (sample_.size() < capacity_) {
      sample_.push_back(std::move(item));
    } else {
      std::uniform_int_distribution<size_t> dist(0, seen_count_ - 1);
      const size_t replace_idx = dist(rng_);
      if (replace_idx < capacity_) {
        sample_[replace_idx] = std::move(item);
      }
    }
  }

  [[nodiscard]] const std::vector<T>& GetSample() const { return sample_; }
  [[nodiscard]] size_t SeenCount() const { return seen_count_; }
  [[nodiscard]] size_t Capacity() const { return capacity_; }

  void Clear() {
    sample_.clear();
    seen_count_ = 0;
  }

 private:
  size_t capacity_;
  size_t seen_count_{0};
  std::vector<T> sample_;
  std::mt19937_64 rng_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_RESERVOIR_SAMPLE_HPP
