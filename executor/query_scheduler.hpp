/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_QUERY_SCHEDULER_HPP
#define TINYLAMB_EXECUTOR_QUERY_SCHEDULER_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "executor/executor_base.hpp"

namespace tinylamb {

struct QuerySchedulerStats {
  uint64_t acquire_count{0};
  uint64_t acquire_wait_ns{0};
  uint64_t contended_acquires{0};
};

class QueryScheduler {
 public:
  class Lease {
   public:
    Lease() = default;
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept;
    Lease& operator=(Lease&& other) noexcept;
    ~Lease();
    void Release();

   private:
    friend class QueryScheduler;
    Lease(QueryScheduler* scheduler, size_t cpu_slots, size_t memory_bytes)
        : scheduler_(scheduler),
          cpu_slots_(cpu_slots),
          memory_bytes_(memory_bytes) {}
    QueryScheduler* scheduler_{nullptr};
    size_t cpu_slots_{0};
    size_t memory_bytes_{0};
  };

  QueryScheduler(size_t cpu_slots, size_t memory_bytes);
  [[nodiscard]] Lease Acquire(size_t cpu_slots, size_t memory_bytes);
  [[nodiscard]] size_t UsedCpuSlots() const;
  [[nodiscard]] size_t UsedMemoryBytes() const;
  [[nodiscard]] size_t CpuCapacity() const { return cpu_capacity_; }
  [[nodiscard]] size_t MemoryCapacity() const { return memory_capacity_; }
  void SetMetricsEnabled(bool enabled) {
    metrics_enabled_.store(enabled, std::memory_order_relaxed);
  }
  [[nodiscard]] QuerySchedulerStats Stats() const;

  static QueryScheduler& Global();

 private:
  void Release(size_t cpu_slots, size_t memory_bytes);

  const size_t cpu_capacity_;
  const size_t memory_capacity_;
  mutable std::mutex mutex_;
  std::condition_variable available_;
  size_t used_cpu_{0};
  size_t used_memory_{0};
  uint64_t next_ticket_{0};
  uint64_t serving_ticket_{0};
  std::atomic<bool> metrics_enabled_{false};
  std::atomic<uint64_t> acquire_count_{0};
  std::atomic<uint64_t> acquire_wait_ns_{0};
  std::atomic<uint64_t> contended_acquires_{0};
};

// Keeps a query-level lease from its first pull until exhaustion or executor
// destruction, so every operator in the plan shares one CPU/memory budget.
class ScheduledExecutor final : public ExecutorBase {
 public:
  ScheduledExecutor(Executor child, QueryScheduler& scheduler, size_t cpu_slots,
                    size_t memory_bytes)
      : child_(std::move(child)),
        scheduler_(&scheduler),
        cpu_slots_(cpu_slots),
        memory_bytes_(memory_bytes) {}

  bool Next(Row* destination, RowPosition* position) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& out, int indent) const override;
  void Explain(std::ostream& out, int indent) const override;

 private:
  void EnsureLease();
  Executor child_;
  QueryScheduler* scheduler_;
  size_t cpu_slots_;
  size_t memory_bytes_;
  std::unique_ptr<QueryScheduler::Lease> lease_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_QUERY_SCHEDULER_HPP
