/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/query_scheduler.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <thread>
#include <utility>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

QueryScheduler::QueryScheduler(size_t cpu_slots, size_t memory_bytes)
    : cpu_capacity_(std::max<size_t>(1, cpu_slots)),
      memory_capacity_(std::max<size_t>(1, memory_bytes)) {}

QueryScheduler::Lease QueryScheduler::Acquire(size_t cpu_slots,
                                              size_t memory_bytes) {
  const bool measure = metrics_enabled_.load(std::memory_order_relaxed);
  const auto wait_start = measure ? std::chrono::steady_clock::now()
                                  : std::chrono::steady_clock::time_point{};
  cpu_slots = std::clamp<size_t>(cpu_slots, 1, cpu_capacity_);
  memory_bytes = std::min(memory_bytes, memory_capacity_);
  std::unique_lock lock(mutex_);
  const uint64_t ticket = next_ticket_++;
  available_.wait(lock, [&] {
    return ticket == serving_ticket_ &&
           used_cpu_ + cpu_slots <= cpu_capacity_ &&
           used_memory_ + memory_bytes <= memory_capacity_;
  });
  used_cpu_ += cpu_slots;
  used_memory_ += memory_bytes;
  ++serving_ticket_;
  if (measure) {
    const uint64_t wait_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - wait_start)
            .count());
    acquire_count_.fetch_add(1, std::memory_order_relaxed);
    acquire_wait_ns_.fetch_add(wait_ns, std::memory_order_relaxed);
    if (wait_ns >= 1000) {
      contended_acquires_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  lock.unlock();
  available_.notify_all();
  return {this, cpu_slots, memory_bytes};
}

QuerySchedulerStats QueryScheduler::Stats() const {
  return {
      .acquire_count = acquire_count_.load(std::memory_order_relaxed),
      .acquire_wait_ns = acquire_wait_ns_.load(std::memory_order_relaxed),
      .contended_acquires = contended_acquires_.load(std::memory_order_relaxed),
  };
}

void QueryScheduler::Release(size_t cpu_slots, size_t memory_bytes) {
  {
    std::scoped_lock lock(mutex_);
    used_cpu_ -= cpu_slots;
    used_memory_ -= memory_bytes;
  }
  available_.notify_all();
}

size_t QueryScheduler::UsedCpuSlots() const {
  std::scoped_lock lock(mutex_);
  return used_cpu_;
}

size_t QueryScheduler::UsedMemoryBytes() const {
  std::scoped_lock lock(mutex_);
  return used_memory_;
}

QueryScheduler& QueryScheduler::Global() {
  constexpr size_t kDefaultMemoryBudget = size_t{1} << 30;
  static QueryScheduler scheduler(std::thread::hardware_concurrency(),
                                  kDefaultMemoryBudget);
  return scheduler;
}

QueryScheduler::Lease::Lease(Lease&& other) noexcept
    : scheduler_(std::exchange(other.scheduler_, nullptr)),
      cpu_slots_(other.cpu_slots_),
      memory_bytes_(other.memory_bytes_) {}

QueryScheduler::Lease& QueryScheduler::Lease::operator=(
    Lease&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  Release();
  scheduler_ = std::exchange(other.scheduler_, nullptr);
  cpu_slots_ = other.cpu_slots_;
  memory_bytes_ = other.memory_bytes_;
  return *this;
}

QueryScheduler::Lease::~Lease() { Release(); }

void QueryScheduler::Lease::Release() {
  if (scheduler_ == nullptr) {
    return;
  }
  scheduler_->Release(cpu_slots_, memory_bytes_);
  scheduler_ = nullptr;
}

void ScheduledExecutor::EnsureLease() {
  if (!lease_) {
    lease_ = std::make_unique<QueryScheduler::Lease>(
        scheduler_->Acquire(cpu_slots_, memory_bytes_));
  }
}

bool ScheduledExecutor::Next(Row* destination, RowPosition* position) {
  EnsureLease();
  const bool produced = child_->Next(destination, position);
  if (!produced) {
    lease_.reset();
  }
  return produced;
}

size_t ScheduledExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  EnsureLease();
  const size_t produced = child_->NextBatch(destination, max_rows);
  if (produced == 0) {
    lease_.reset();
  }
  return produced;
}

void ScheduledExecutor::Dump(std::ostream& out, int indent) const {
  out << "ScheduledQuery (cpu=" << cpu_slots_ << ", memory=" << memory_bytes_
      << ")\n"
      << Indent(indent + 2);
  child_->Dump(out, indent + 2);
}

void ScheduledExecutor::Explain(std::ostream& out, int indent) const {
  out << "ScheduledQuery (cpu=" << cpu_slots_ << ", memory=" << memory_bytes_
      << ")\n"
      << Indent(indent + 2);
  child_->Explain(out, indent + 2);
}

}  // namespace tinylamb
