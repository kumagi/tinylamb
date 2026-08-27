/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_HYPER_LOG_LOG_HPP
#define TINYLAMB_HYPER_LOG_LOG_HPP

#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "type/value.hpp"

namespace tinylamb {

class HyperLogLog {
 public:
  explicit HyperLogLog(uint8_t precision = 10)
      : precision_(precision),
        num_registers_(1ULL << precision),
        registers_(num_registers_, 0) {}

  void Add(uint64_t hash);
  void Add(std::string_view data);
  void Add(const Value& value);

  [[nodiscard]] double Estimate() const;
  void Merge(const HyperLogLog& other);
  void Clear();

  [[nodiscard]] uint8_t Precision() const { return precision_; }
  [[nodiscard]] size_t RegisterCount() const { return num_registers_; }
  [[nodiscard]] const std::vector<uint8_t>& Registers() const {
    return registers_;
  }

 private:
  [[nodiscard]] double AlphaM() const;

  uint8_t precision_;
  size_t num_registers_;
  std::vector<uint8_t> registers_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_HYPER_LOG_LOG_HPP
