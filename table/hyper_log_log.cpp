/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "table/hyper_log_log.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <string_view>
#include <vector>

namespace tinylamb {
namespace {

uint64_t Mix64(uint64_t k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccdULL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53ULL;
  k ^= k >> 33;
  return k;
}

uint64_t Fnv1a64(std::string_view data) {
  uint64_t hash = 14695981039346656037ULL;
  for (unsigned char c : data) {
    hash ^= static_cast<uint64_t>(c);
    hash *= 1099511628211ULL;
  }
  return Mix64(hash);
}

}  // namespace

void HyperLogLog::Add(uint64_t hash) {
  hash = Mix64(hash);
  const size_t index = hash >> (64 - precision_);
  const uint64_t remaining = hash << precision_;
  const uint8_t leading_zeros = remaining == 0
      ? static_cast<uint8_t>(64 - precision_ + 1)
      : static_cast<uint8_t>(std::countl_zero(remaining) + 1);
  registers_[index] = std::max(registers_[index], leading_zeros);
}

void HyperLogLog::Add(std::string_view data) {
  Add(Fnv1a64(data));
}

void HyperLogLog::Add(const Value& value) {
  if (value.IsNull()) {
    Add(Fnv1a64("\x00NULL"));
  } else {
    Add(Fnv1a64(value.AsString()));
  }
}

double HyperLogLog::AlphaM() const {
  if (num_registers_ == 16) {
    return 0.673;
  }
  if (num_registers_ == 32) {
    return 0.697;
  }
  if (num_registers_ == 64) {
    return 0.709;
  }
  return 0.7213 / (1.0 + 1.079 / static_cast<double>(num_registers_));
}

double HyperLogLog::Estimate() const {
  double sum = 0.0;
  size_t zero_registers = 0;
  for (uint8_t val : registers_) {
    sum += std::ldexp(1.0, -val);
    if (val == 0) {
      ++zero_registers;
    }
  }

  const double m = static_cast<double>(num_registers_);
  double raw_estimate = AlphaM() * m * m / sum;

  // Small range correction (LinearCounting)
  if (raw_estimate <= 2.5 * m && zero_registers > 0) {
    return m * std::log(m / static_cast<double>(zero_registers));
  }

  return raw_estimate;
}

void HyperLogLog::Merge(const HyperLogLog& other) {
  if (precision_ != other.precision_) {
    return;
  }
  for (size_t i = 0; i < num_registers_; ++i) {
    registers_[i] = std::max(registers_[i], other.registers_[i]);
  }
}

void HyperLogLog::Clear() {
  std::fill(registers_.begin(), registers_.end(), 0);
}

}  // namespace tinylamb
