/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_OPERATOR_MEMORY_HPP
#define TINYLAMB_EXECUTOR_OPERATOR_MEMORY_HPP

#include <atomic>
#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

#include "executor/query_memory.hpp"

namespace tinylamb {

// Per-operator working memory reservation and limit manager.
// Tracks memory consumption, triggers spill / graceful degradation callbacks
// when quotas are exceeded, and interfaces with the process-wide
// QueryMemoryBudget.
class OperatorMemoryReservation {
 public:
  OperatorMemoryReservation() = default;
  explicit OperatorMemoryReservation(
      std::string_view operator_name, size_t limit_bytes = 0,
      std::function<void()> spill_callback = nullptr);

  ~OperatorMemoryReservation();

  OperatorMemoryReservation(const OperatorMemoryReservation&) = delete;
  OperatorMemoryReservation& operator=(const OperatorMemoryReservation&) =
      delete;
  OperatorMemoryReservation(OperatorMemoryReservation&& other) noexcept;
  OperatorMemoryReservation& operator=(
      OperatorMemoryReservation&& other) noexcept;

  // Attempts to allocate `bytes`. If `limit_bytes_ > 0` and total exceeds
  // limit, invokes `spill_callback_` (if registered), marks degraded, and
  // returns false.
  bool TryAllocate(size_t bytes);

  // Allocates memory regardless of limits (e.g. required structures), updating
  // stats.
  void AllocateForced(size_t bytes);

  // Releases `bytes` of previously allocated memory.
  void Free(size_t bytes);

  // Releases all allocated memory.
  void Reset();

  [[nodiscard]] size_t AllocatedBytes() const { return allocated_; }
  [[nodiscard]] size_t Limit() const { return limit_; }
  [[nodiscard]] bool HasLimit() const { return limit_ > 0; }
  [[nodiscard]] bool ShouldSpill() const {
    return (limit_ > 0 && allocated_ >= limit_) || degraded_;
  }
  [[nodiscard]] bool IsDegraded() const { return degraded_; }
  void SetDegraded(bool degraded) { degraded_ = degraded; }

  void SetLimit(size_t limit_bytes) { limit_ = limit_bytes; }
  void SetSpillCallback(std::function<void()> callback) {
    spill_callback_ = std::move(callback);
  }

  [[nodiscard]] const std::string& OperatorName() const { return name_; }

 private:
  std::string name_;
  size_t limit_{0};
  size_t allocated_{0};
  bool degraded_{false};
  std::function<void()> spill_callback_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_OPERATOR_MEMORY_HPP
