/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_QUERY_GRANT_HPP
#define TINYLAMB_COMMON_QUERY_GRANT_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

// Holds a query-level memory admission grant G_q reserved from
// ProcessMemoryBudget pool kQuery (ADR 0004). Active blocking consumers share
// G_q dynamically; when consumers finish, their share shifts automatically to
// downstream stages.
class QueryMemoryGrant {
 public:
  class ActiveConsumerGuard {
   public:
    ActiveConsumerGuard() = default;
    explicit ActiveConsumerGuard(QueryMemoryGrant* grant);
    ActiveConsumerGuard(const ActiveConsumerGuard&) = delete;
    ActiveConsumerGuard& operator=(const ActiveConsumerGuard&) = delete;
    ActiveConsumerGuard(ActiveConsumerGuard&& other) noexcept
        : grant_(other.grant_), alive_(std::move(other.alive_)) {
      other.grant_ = nullptr;
    }
    ActiveConsumerGuard& operator=(ActiveConsumerGuard&& other) noexcept {
      if (this != &other) {
        Reset();
        grant_ = other.grant_;
        alive_ = std::move(other.alive_);
        other.grant_ = nullptr;
      }
      return *this;
    }
    ~ActiveConsumerGuard() { Reset(); }

    void Reset();

   private:
    QueryMemoryGrant* grant_{nullptr};
    std::weak_ptr<const bool> alive_;
  };

  // Worker-local chunk reservation cache.  Callers size the refill between
  // a 64 KiB floor and kDefaultRefillBytes (256 KiB) below, so hot loops
  // avoid per-row atomic traffic against QueryMemoryGrant (ADR 0003).
  class LocalBatchCache {
   public:
    static constexpr size_t kDefaultRefillBytes = size_t{256} << 10;

    explicit LocalBatchCache(QueryMemoryGrant* grant = nullptr,
                             size_t refill_bytes = kDefaultRefillBytes);
    LocalBatchCache(const LocalBatchCache&) = delete;
    LocalBatchCache& operator=(const LocalBatchCache&) = delete;
    LocalBatchCache(LocalBatchCache&& other) noexcept
        : grant_(other.grant_),
          alive_(std::move(other.alive_)),
          refill_bytes_(other.refill_bytes_),
          cached_bytes_(other.cached_bytes_),
          held_bytes_(other.held_bytes_) {
      other.grant_ = nullptr;
      other.cached_bytes_ = 0;
      other.held_bytes_ = 0;
    }
    LocalBatchCache& operator=(LocalBatchCache&& other) noexcept {
      if (this != &other) {
        ReleaseAll();
        grant_ = other.grant_;
        alive_ = std::move(other.alive_);
        refill_bytes_ = other.refill_bytes_;
        cached_bytes_ = other.cached_bytes_;
        held_bytes_ = other.held_bytes_;
        other.grant_ = nullptr;
        other.cached_bytes_ = 0;
        other.held_bytes_ = 0;
      }
      return *this;
    }
    ~LocalBatchCache() { ReleaseAll(); }

    [[nodiscard]] bool Reserve(size_t bytes);
    void Release(size_t bytes);
    void ReleaseAll();
    [[nodiscard]] size_t CachedBytes() const noexcept { return cached_bytes_; }
    [[nodiscard]] size_t HeldBytes() const noexcept { return held_bytes_; }

   private:
    QueryMemoryGrant* grant_{nullptr};
    std::weak_ptr<const bool> alive_;
    size_t refill_bytes_{kDefaultRefillBytes};
    size_t cached_bytes_{0};
    size_t held_bytes_{0};
  };

  QueryMemoryGrant() = default;
  QueryMemoryGrant(const QueryMemoryGrant&) = delete;
  QueryMemoryGrant& operator=(const QueryMemoryGrant&) = delete;
  QueryMemoryGrant(QueryMemoryGrant&& other) noexcept;
  QueryMemoryGrant& operator=(QueryMemoryGrant&& other) noexcept;
  ~QueryMemoryGrant() {
    alive_token_.reset();
    ReleaseGrant();
  }

  [[nodiscard]] bool Unlimited() const noexcept {
    return granted_bytes_.load(std::memory_order_acquire) == 0 && !limited_;
  }
  [[nodiscard]] size_t GrantedBytes() const noexcept {
    return granted_bytes_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t UsedBytes() const noexcept {
    return used_bytes_.load(std::memory_order_acquire);
  }
  [[nodiscard]] uint64_t WaitNs() const noexcept { return wait_ns_; }
  [[nodiscard]] size_t ActiveConsumerCount() const noexcept {
    return active_consumers_.load(std::memory_order_acquire);
  }

  // Computes per-operator working memory budget B_op under G_q (ADR 0004).
  [[nodiscard]] size_t OperatorBudgetBytes(size_t b_min = 0) const noexcept;

  // Single-point memory-aware DOP throttling (ADR 0004): reduces W when
  // B_op / W < b_min.
  [[nodiscard]] static size_t ComputeEffectiveDop(size_t requested_dop,
                                                  size_t b_op_bytes,
                                                  size_t b_min_bytes) noexcept;

  // Atomic sub-reservation / release inside the query's granted budget.
  [[nodiscard]] bool TryConsume(size_t bytes) noexcept;
  void ReleaseConsumed(size_t bytes) noexcept;
  void ReleaseGrant() noexcept;

  [[nodiscard]] ActiveConsumerGuard TrackActiveConsumer() {
    return ActiveConsumerGuard(this);
  }
  [[nodiscard]] std::weak_ptr<const bool> LifetimeToken() const noexcept {
    return alive_token_;
  }

 private:
  friend class QueryMemoryCoordinator;
  QueryMemoryGrant(ProcessMemoryBudget* budget, size_t granted_bytes,
                   bool limited, uint64_t wait_ns);

  // Forward only new high-water marks to the process-wide telemetry. Doing a
  // global atomic-max CAS on every tiny TryConsume made parallel operators
  // contend on one cache line per batch refill.
  void RecordPeak(size_t used) noexcept;

  std::shared_ptr<const bool> alive_token_{std::make_shared<const bool>(true)};
  ProcessMemoryBudget* budget_{nullptr};
  std::atomic<size_t> granted_bytes_{0};
  std::atomic<size_t> used_bytes_{0};
  std::atomic<size_t> reported_peak_bytes_{0};
  std::atomic<size_t> active_consumers_{0};
  bool limited_{false};
  uint64_t wait_ns_{0};
};

// Coordinates query-level memory grant admission (G_q) across concurrent
// queries and notifies waiters on memory release (ADR 0004, ADR 0012).
class QueryMemoryCoordinator {
 public:
  static constexpr size_t kDefaultMaxGrantBytes = size_t{8} << 30;  // 8 GiB
  static constexpr uint64_t kDefaultWaitTimeoutMs = 30000;

  static QueryMemoryCoordinator& Global();

  explicit QueryMemoryCoordinator(
      ProcessMemoryBudget* budget = &ProcessMemoryBudget::Global());
  QueryMemoryCoordinator(const QueryMemoryCoordinator&) = delete;
  QueryMemoryCoordinator& operator=(const QueryMemoryCoordinator&) = delete;
  QueryMemoryCoordinator(QueryMemoryCoordinator&&) = delete;
  QueryMemoryCoordinator& operator=(QueryMemoryCoordinator&&) = delete;
  ~QueryMemoryCoordinator();

  [[nodiscard]] size_t MaxGrantFromEnv() const noexcept;
  [[nodiscard]] static uint64_t WaitTimeoutMsFromEnv() noexcept;

  // Admits a query requesting between [min_bytes, preferred_bytes].
  // `preferred_bytes == 0 && min_bytes == 0` admits immediately without consuming pool quota.
  [[nodiscard]] StatusOr<QueryMemoryGrant> AcquireGrant(
      size_t preferred_bytes, size_t min_bytes = 0,
      std::optional<uint64_t> wait_timeout_ms = std::nullopt);

  void NotifyReleased();

 private:
  // The wake-up target lives behind a shared_ptr: the release listener is a
  // std::function *copy* living in ProcessMemoryBudget's atomic view, so a
  // concurrent (or late) NotifyReleaseListeners can invoke it while or after
  // ~QueryMemoryCoordinator runs. The lambda holds only a weak_ptr, so once
  // the coordinator is gone a stale call locks nothing and no-ops instead of
  // touching a freed mutex/cv. Waiters keep their own strong reference for
  // the duration of the wait (the coordinator itself must still outlive any
  // waiter -- unchanged contract, mu_ stays a member).
  struct ReleaseSignal {
    std::condition_variable cv;
  };

  ProcessMemoryBudget* budget_;
  std::mutex mu_;
  std::shared_ptr<ReleaseSignal> signal_{std::make_shared<ReleaseSignal>()};
  uint64_t release_listener_id_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_QUERY_GRANT_HPP
