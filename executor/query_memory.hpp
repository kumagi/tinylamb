/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_QUERY_MEMORY_HPP
#define TINYLAMB_QUERY_MEMORY_HPP

#include <atomic>
#include <cstddef>
#include <string>

#include "type/row.hpp"

namespace tinylamb {

// Process-wide query working-memory budget. Operators should estimate growth
// with EstimateRowBytes / TryReserve and spill before the OS starts thrashing.
//
// Config: TINYLAMB_QUERY_MEMORY_BYTES
//   - unset: default 1 GiB
//   - "0": unlimited (legacy / debugging)
class QueryMemoryBudget {
 public:
  static QueryMemoryBudget& Global();

  [[nodiscard]] size_t Limit() const { return limit_; }
  [[nodiscard]] bool Unlimited() const { return limit_ == 0; }
  [[nodiscard]] size_t Used() const {
    return used_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] size_t Remaining() const;

  // Returns false when `bytes` would push usage past the soft limit (80%).
  // Does not mutate `used_`; pair with ReserveForced / QueryMemoryCharge::Add.
  [[nodiscard]] bool CanReserve(size_t bytes) const;
  void ReserveForced(size_t bytes);
  void Release(size_t bytes);

  // Test helper: replace the limit (0 = unlimited).
  void ResetForTest(size_t limit_bytes);

 private:
  explicit QueryMemoryBudget(size_t limit_bytes);
  static size_t LimitFromEnv();

  size_t limit_;
  std::atomic<size_t> used_{0};
  static constexpr size_t kSoftFractionNum = 4;
  static constexpr size_t kSoftFractionDen = 5;  // spill at 80%
};

[[nodiscard]] size_t EstimateValueBytes(const Value& value);
[[nodiscard]] size_t EstimateRowBytes(const Row& row);

// RAII charge against the global budget.
class QueryMemoryCharge {
 public:
  QueryMemoryCharge() = default;
  explicit QueryMemoryCharge(size_t bytes);
  QueryMemoryCharge(const QueryMemoryCharge&) = delete;
  QueryMemoryCharge& operator=(const QueryMemoryCharge&) = delete;
  QueryMemoryCharge(QueryMemoryCharge&& other) noexcept;
  QueryMemoryCharge& operator=(QueryMemoryCharge&& other) noexcept;
  ~QueryMemoryCharge();

  void Add(size_t bytes);
  void ReleaseAll();
  [[nodiscard]] size_t Bytes() const { return bytes_; }

 private:
  size_t bytes_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_MEMORY_HPP
