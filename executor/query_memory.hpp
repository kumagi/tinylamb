/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_QUERY_MEMORY_HPP
#define TINYLAMB_QUERY_MEMORY_HPP

#include <atomic>
#include <cstddef>
#include <limits>
#include <string>

#include "common/memory_budget.hpp"
#include "type/row.hpp"

namespace tinylamb {

// Process-wide query working-memory budget. Operators should estimate growth
// with EstimateRowBytes / TryReserve and spill before the OS starts thrashing.
// Coordinates with ProcessMemoryBudget so storage + columnar + query buffers
// stay within TINYLAMB_MEMORY_LIMIT_BYTES (16 GiB default).
//
// Config: TINYLAMB_QUERY_MEMORY_BYTES
//   - unset: default 8 GiB, clamped to the process-level memory limit
//   - "0": disable the query-specific cap, still bounded by the process limit
class QueryMemoryBudget {
 public:
  static QueryMemoryBudget& Global();

  [[nodiscard]] size_t Limit() const { return limit_; }
  [[nodiscard]] bool Unlimited() const { return limit_ == 0; }
  [[nodiscard]] size_t Used() const {
    return used_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t Remaining() const;

  // Returns false when `bytes` would push usage past the soft limit (80%) or
  // when ProcessMemoryBudget signals RSS/global pressure.
  [[nodiscard]] bool CanReserve(size_t bytes) const;
  // Atomically checks CanReserve-equivalent headroom and reserves `bytes`.
  [[nodiscard]] bool TryReserve(size_t bytes);
  void ReserveForced(size_t bytes);
  void Release(size_t bytes);

  // Test helper: replace the limit (0 = unlimited). By default keeps linkage
  // to ProcessMemoryBudget::Global(); pass `isolate_from_process = true`
  // only in standalone unit tests of QueryMemoryBudget itself.
  void ResetForTest(size_t limit_bytes, bool isolate_from_process = false);

 private:
  explicit QueryMemoryBudget(size_t limit_bytes);
  static size_t LimitFromEnv();

  size_t limit_;
  // 80% of limit_, precomputed: CanReserve runs once per buffered row.
  size_t soft_{0};
  std::atomic<size_t> used_{0};
  bool test_isolated_{false};
  static constexpr size_t kSoftFractionNum = 4;
  static constexpr size_t kSoftFractionDen = 5;  // spill at 80%
};

[[nodiscard]] size_t EstimateValueBytes(const Value& value);
[[nodiscard]] size_t EstimateRowBytes(const Row& row);

[[nodiscard]] inline size_t EstimateScalarValueBytes(const Value& value) {
  constexpr size_t kBase = 32;
  switch (value.type) {
    case ValueType::kNull:
      return kBase;
    case ValueType::kInt64:
    case ValueType::kDouble:
    case ValueType::kDate:
      return kBase + 8;
    case ValueType::kVarChar:
      return kBase + value.value.varchar_value.size();
    default:
      return kBase + 16;
  }
}

[[nodiscard]] inline size_t EstimateRowBytesFast(const Row& row) {
  size_t total = 64;  // vector / Row overhead
  for (const Value& value : row.values_) {
    total += value.type == ValueType::kArray ? EstimateValueBytes(value)
                                             : EstimateScalarValueBytes(value);
  }
  return total;
}

// RAII charge against the global budget.
class QueryMemoryCharge {
 public:
  // Tag for adopting bytes that were already atomically reserved via
  // QueryMemoryBudget::TryReserve (T3724): the reservation must not be
  // forced through ReserveForced a second time.
  struct kAlreadyReserved {};
  QueryMemoryCharge() = default;
  explicit QueryMemoryCharge(size_t bytes);
  QueryMemoryCharge(kAlreadyReserved /*unused*/, size_t bytes) noexcept
      : bytes_(bytes) {}  // NOLINT(readability-named-parameter)
  QueryMemoryCharge(const QueryMemoryCharge&) = delete;
  QueryMemoryCharge& operator=(const QueryMemoryCharge&) = delete;
  QueryMemoryCharge(QueryMemoryCharge&& other) noexcept;
  QueryMemoryCharge& operator=(QueryMemoryCharge&& other) noexcept;
  ~QueryMemoryCharge();

  // Force-charge as many bytes as this owner can account for without overflow.
  void Add(size_t bytes);
  [[nodiscard]] bool TryAdd(size_t bytes);
  void Release(size_t bytes);
  void Adopt(size_t bytes) noexcept {
    // Adopt is used with bytes that were already reserved through
    // QueryMemoryBudget::TryReserve. Saturate instead of wrapping: a wrapped
    // bytes_ would make ReleaseAll hand back far too little to the process
    // budget and leak the reservation permanently (QueryMemoryBudget::Release
    // already clamps an over-release to the current usage).
    constexpr size_t kMax = std::numeric_limits<size_t>::max();
    bytes_ = bytes > kMax - bytes_ ? kMax : bytes_ + bytes;
  }
  // Transfers responsibility for releasing the reservation to the caller.
  // The global budget keeps the bytes reserved. Immediately adopt them into
  // another QueryMemoryCharge or release them explicitly with the budget.
  [[nodiscard]] size_t DetachBytes() noexcept {
    const size_t detached = bytes_;
    bytes_ = 0;
    return detached;
  }
  void ReleaseAll();
  [[nodiscard]] size_t Bytes() const { return bytes_; }

 private:
  size_t bytes_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_MEMORY_HPP
