/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/operator_memory.hpp"

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

#include "executor/query_memory.hpp"

namespace tinylamb {

OperatorMemoryReservation::OperatorMemoryReservation(
    std::string_view operator_name, size_t limit_bytes,
    std::function<void()> spill_callback)
    : name_(operator_name),
      limit_(limit_bytes),
      spill_callback_(std::move(spill_callback)) {}

OperatorMemoryReservation::~OperatorMemoryReservation() { Reset(); }

OperatorMemoryReservation::OperatorMemoryReservation(
    OperatorMemoryReservation&& other) noexcept
    : name_(std::move(other.name_)),
      limit_(other.limit_),
      allocated_(other.allocated_),
      degraded_(other.degraded_),
      spill_callback_(std::move(other.spill_callback_)) {
  other.allocated_ = 0;
  other.limit_ = 0;
  other.degraded_ = false;
}

OperatorMemoryReservation& OperatorMemoryReservation::operator=(
    OperatorMemoryReservation&& other) noexcept {
  if (this != &other) {
    Reset();
    name_ = std::move(other.name_);
    limit_ = other.limit_;
    allocated_ = other.allocated_;
    degraded_ = other.degraded_;
    spill_callback_ = std::move(other.spill_callback_);
    other.allocated_ = 0;
    other.limit_ = 0;
    other.degraded_ = false;
  }
  return *this;
}

bool OperatorMemoryReservation::TryAllocate(size_t bytes) {
  if (limit_ > 0 && allocated_ + bytes > limit_) {
    degraded_ = true;
    if (spill_callback_) {
      spill_callback_();
    }
    return false;
  }
  allocated_ += bytes;
  QueryMemoryBudget::Global().ReserveForced(bytes);
  return true;
}

void OperatorMemoryReservation::AllocateForced(size_t bytes) {
  allocated_ += bytes;
  QueryMemoryBudget::Global().ReserveForced(bytes);
  if (limit_ > 0 && allocated_ > limit_) {
    degraded_ = true;
  }
}

void OperatorMemoryReservation::Free(size_t bytes) {
  const size_t actual = std::min(allocated_, bytes);
  allocated_ -= actual;
  QueryMemoryBudget::Global().Release(actual);
}

void OperatorMemoryReservation::Reset() {
  if (allocated_ > 0) {
    QueryMemoryBudget::Global().Release(allocated_);
    allocated_ = 0;
  }
  degraded_ = false;
}

}  // namespace tinylamb
