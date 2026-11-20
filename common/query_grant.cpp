/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/query_grant.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <string>

namespace tinylamb {
namespace {

size_t ReadGrantEnvU64(const char* name, size_t fallback) noexcept {
  const char* raw = std::getenv(name);
  if (raw == nullptr || raw[0] == '\0') {
    return fallback;
  }
  if (raw[0] == '-' || raw[0] == '+') {
    return fallback;
  }
  char* end = nullptr;
  errno = 0;
  const unsigned long long parsed = std::strtoull(raw, &end, 10);
  if (errno != 0 || end == raw || *end != '\0' ||
      parsed > std::numeric_limits<size_t>::max()) {
    return fallback;
  }
  return static_cast<size_t>(parsed);
}

}  // namespace

QueryMemoryGrant::ActiveConsumerGuard::ActiveConsumerGuard(
    QueryMemoryGrant* grant)
    : grant_(grant),
      alive_(grant != nullptr ? grant->LifetimeToken()
                              : std::weak_ptr<const bool>{}) {
  if (grant_ != nullptr) {
    grant_->active_consumers_.fetch_add(1, std::memory_order_acq_rel);
  }
}

void QueryMemoryGrant::ActiveConsumerGuard::Reset() {
  if (grant_ != nullptr) {
    if (const auto live = alive_.lock()) {
      size_t cur = grant_->active_consumers_.load(std::memory_order_acquire);
      while (cur > 0 && !grant_->active_consumers_.compare_exchange_weak(
                            cur, cur - 1, std::memory_order_acq_rel,
                            std::memory_order_acquire)) {
      }
    }
    grant_ = nullptr;
    alive_.reset();
  }
}

QueryMemoryGrant::LocalBatchCache::LocalBatchCache(QueryMemoryGrant* grant,
                                                   size_t refill_bytes)
    : grant_(grant),
      alive_(grant != nullptr ? grant->LifetimeToken()
                              : std::weak_ptr<const bool>{}),
      refill_bytes_(refill_bytes) {}

bool QueryMemoryGrant::LocalBatchCache::Reserve(size_t bytes) {
  if (bytes == 0) {
    return true;
  }
  if (cached_bytes_ >= bytes) {
    cached_bytes_ -= bytes;
    held_bytes_ += bytes;
    return true;
  }
  if (grant_ == nullptr) {
    held_bytes_ += bytes;
    return true;
  }
  const auto live = alive_.lock();
  if (!live) {
    return false;
  }
  const size_t deficit = bytes - cached_bytes_;
  const size_t chunk = std::max(deficit, refill_bytes_);
  if (grant_->TryConsume(chunk)) {
    cached_bytes_ += chunk - bytes;
    held_bytes_ += bytes;
    return true;
  }
  if (chunk != deficit && grant_->TryConsume(deficit)) {
    cached_bytes_ = 0;
    held_bytes_ += bytes;
    return true;
  }
  return false;
}

void QueryMemoryGrant::LocalBatchCache::Release(size_t bytes) {
  const size_t n = std::min(bytes, held_bytes_);
  if (n == 0) {
    return;
  }
  held_bytes_ -= n;
  cached_bytes_ += n;
  // Saturating doubling: a huge refill_bytes_ would wrap in size_t and make
  // the threshold spuriously small, corrupting the released-byte accounting.
  const size_t threshold =
      refill_bytes_ > std::numeric_limits<size_t>::max() / 2
          ? std::numeric_limits<size_t>::max()
          : refill_bytes_ * 2;
  if (cached_bytes_ > threshold && grant_ != nullptr) {
    const size_t excess = cached_bytes_ - refill_bytes_;
    cached_bytes_ -= excess;
    if (const auto live = alive_.lock()) {
      grant_->ReleaseConsumed(excess);
    }
  }
}

void QueryMemoryGrant::LocalBatchCache::ReleaseAll() {
  const size_t total = held_bytes_ + cached_bytes_;
  held_bytes_ = 0;
  cached_bytes_ = 0;
  if (total > 0 && grant_ != nullptr) {
    if (const auto live = alive_.lock()) {
      grant_->ReleaseConsumed(total);
    }
  }
}

QueryMemoryGrant::QueryMemoryGrant(ProcessMemoryBudget* budget,
                                   size_t granted_bytes, bool limited,
                                   uint64_t wait_ns)
    : budget_(budget),
      granted_bytes_(granted_bytes),
      limited_(limited),
      wait_ns_(wait_ns) {}

QueryMemoryGrant::QueryMemoryGrant(QueryMemoryGrant&& other) noexcept
    : alive_token_(std::move(other.alive_token_)),
      budget_(other.budget_),
      granted_bytes_(other.granted_bytes_.load(std::memory_order_acquire)),
      used_bytes_(other.used_bytes_.load(std::memory_order_acquire)),
      active_consumers_(
          other.active_consumers_.load(std::memory_order_acquire)),
      limited_(other.limited_),
      wait_ns_(other.wait_ns_) {
  other.budget_ = nullptr;
  other.granted_bytes_.store(0, std::memory_order_release);
  other.used_bytes_.store(0, std::memory_order_release);
  other.active_consumers_.store(0, std::memory_order_release);
  other.limited_ = false;
  other.wait_ns_ = 0;
}

QueryMemoryGrant& QueryMemoryGrant::operator=(
    QueryMemoryGrant&& other) noexcept {
  if (this != &other) {
    alive_token_.reset();
    ReleaseGrant();
    alive_token_ = std::move(other.alive_token_);
    budget_ = other.budget_;
    granted_bytes_.store(other.granted_bytes_.load(std::memory_order_acquire),
                         std::memory_order_release);
    used_bytes_.store(other.used_bytes_.load(std::memory_order_acquire),
                      std::memory_order_release);
    active_consumers_.store(
        other.active_consumers_.load(std::memory_order_acquire),
        std::memory_order_release);
    limited_ = other.limited_;
    wait_ns_ = other.wait_ns_;
    other.budget_ = nullptr;
    other.granted_bytes_.store(0, std::memory_order_release);
    other.used_bytes_.store(0, std::memory_order_release);
    other.active_consumers_.store(0, std::memory_order_release);
    other.limited_ = false;
    other.wait_ns_ = 0;
  }
  return *this;
}

size_t QueryMemoryGrant::OperatorBudgetBytes(size_t b_min) const noexcept {
  const size_t override_bytes =
      ProcessMemoryBudget::Global().OpMemoryOverrideBytes();
  if (override_bytes != 0) {
    return b_min == 0 ? override_bytes : std::max(b_min, override_bytes);
  }
  const size_t grant = granted_bytes_.load(std::memory_order_acquire);
  if (grant == 0) {
    const size_t ceiling = QueryMemoryCoordinator::Global().MaxGrantFromEnv();
    return b_min == 0 ? ceiling : std::max(b_min, ceiling);
  }
  const size_t active =
      std::max<size_t>(1, active_consumers_.load(std::memory_order_acquire));
  // Hand out only the fair share: applying the b_min floor here can make
  // active * b_min exceed the whole grant, so operators see a larger budget
  // than the query actually has and delay spilling. If the share is below
  // b_min the query is simply over-subscribed; TryConsume still enforces the
  // hard grant ceiling.
  return grant / active;
}

size_t QueryMemoryGrant::ComputeEffectiveDop(size_t requested_dop,
                                             size_t b_op_bytes,
                                             size_t b_min_bytes) noexcept {
  const size_t dop = std::max<size_t>(1, requested_dop);
  if (b_min_bytes == 0 || b_op_bytes == 0) {
    return dop;
  }
  const size_t max_workers = std::max<size_t>(1, b_op_bytes / b_min_bytes);
  return std::min(dop, max_workers);
}

void QueryMemoryGrant::RecordPeak(size_t used) noexcept {
  // RecordOperatorPeakBytes is itself a global atomic max over one cache line
  // (plus a thread-local scope-chain walk). Only publish when this grant saw
  // a genuinely new peak; repeated small consumes below the old peak need no
  // cross-thread traffic at all.
  size_t peak = reported_peak_bytes_.load(std::memory_order_relaxed);
  while (used > peak &&
         !reported_peak_bytes_.compare_exchange_weak(
             peak, used, std::memory_order_relaxed, std::memory_order_relaxed)) {
  }
  if (used > peak) {
    ProcessMemoryBudget::Global().RecordOperatorPeakBytes(used);
  }
}

bool QueryMemoryGrant::TryConsume(size_t bytes) noexcept {
  if (bytes == 0) {
    return true;
  }
  if (ProcessMemoryBudget::Global().ConsumeInjectedMemoryFailure()) {
    return false;
  }
  const size_t limit = granted_bytes_.load(std::memory_order_acquire);
  if (!limited_ || limit == 0) {
    const size_t used_after = used_bytes_.fetch_add(bytes, std::memory_order_acq_rel) + bytes;
    RecordPeak(used_after);
    return true;
  }
  size_t cur = used_bytes_.load(std::memory_order_acquire);
  while (true) {
    if (cur > limit || bytes > limit - cur) {
      return false;
    }
    if (used_bytes_.compare_exchange_weak(cur, cur + bytes,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
      RecordPeak(cur + bytes);
      return true;
    }
  }
}

void QueryMemoryGrant::ReleaseConsumed(size_t bytes) noexcept {
  if (bytes == 0) {
    return;
  }
  size_t cur = used_bytes_.load(std::memory_order_acquire);
  while (cur != 0) {
    const size_t next = cur > bytes ? cur - bytes : 0;
    if (used_bytes_.compare_exchange_weak(cur, next, std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
      break;
    }
  }
}

void QueryMemoryGrant::ReleaseGrant() noexcept {
  const size_t granted = granted_bytes_.exchange(0, std::memory_order_acq_rel);
  used_bytes_.store(0, std::memory_order_release);
  if (budget_ != nullptr && granted > 0) {
    budget_->Release(MemoryPoolKind::kQuery, granted);
  }
  budget_ = nullptr;
  limited_ = false;
}

QueryMemoryCoordinator& QueryMemoryCoordinator::Global() {
  static QueryMemoryCoordinator instance(&ProcessMemoryBudget::Global());
  return instance;
}

QueryMemoryCoordinator::QueryMemoryCoordinator(ProcessMemoryBudget* budget)
    : budget_(budget) {
  if (budget_ != nullptr) {
    std::weak_ptr<ReleaseSignal> weak_signal = signal_;
    release_listener_id_ = budget_->RegisterReleaseListener([weak_signal] {
      if (const auto sig = weak_signal.lock()) {
        sig->cv.notify_all();
      }
    });
  }
}

QueryMemoryCoordinator::~QueryMemoryCoordinator() {
  if (budget_ != nullptr && release_listener_id_ != 0) {
    budget_->UnregisterReleaseListener(release_listener_id_);
  }
}

size_t QueryMemoryCoordinator::MaxGrantFromEnv() const noexcept {
  const size_t proc_max =
      budget_ != nullptr
          ? budget_->EffectivePoolMaxBytes(MemoryPoolKind::kQuery)
          : kDefaultMaxGrantBytes;
  const size_t fallback = std::min(kDefaultMaxGrantBytes, proc_max);
  const size_t requested =
      ReadGrantEnvU64("TINYLAMB_QUERY_MEMORY_BYTES", fallback);
  if (requested == 0) {
    return 0;
  }
  return budget_ != nullptr && !budget_->Unlimited()
             ? std::min(requested, proc_max)
             : requested;
}

uint64_t QueryMemoryCoordinator::WaitTimeoutMsFromEnv() noexcept {
  const uint64_t raw =
      ReadGrantEnvU64("TINYLAMB_GRANT_WAIT_TIMEOUT_MS", kDefaultWaitTimeoutMs);
  // The deadline arithmetic converts this to std::chrono::milliseconds'
  // signed rep; values above INT64_MAX would wrap negative and make the
  // wait expire immediately.
  return std::min<uint64_t>(raw,
                            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
}

void QueryMemoryCoordinator::NotifyReleased() {
  // The wake-up is pure: waiters re-run their reservation attempt when they
  // resume, and there is no state mutated under mu_ for them to observe.
  // Notifying while holding mu_ only made every woken waiter block again on
  // this same mutex.
  signal_->cv.notify_all();
}

StatusOr<QueryMemoryGrant> QueryMemoryCoordinator::AcquireGrant(
    size_t preferred_bytes, size_t min_bytes,
    std::optional<uint64_t> wait_timeout_ms) {
  if (preferred_bytes == 0 && min_bytes == 0) {
    return QueryMemoryGrant(budget_, /*granted_bytes=*/0, /*limited=*/false, 0);
  }
  const size_t target_max = std::max(preferred_bytes, min_bytes);
  const size_t floor_min = std::min(min_bytes, target_max);
  const size_t pool_ceiling =
      budget_ != nullptr
          ? budget_->EffectivePoolMaxBytes(MemoryPoolKind::kQuery)
          : std::numeric_limits<size_t>::max();
  if (budget_ != nullptr && !budget_->Unlimited() && floor_min > pool_ceiling) {
    return StatusError(
        StatusCode::kMemoryLimitExceeded,
        "query minimum memory grant " + std::to_string(floor_min) +
            " bytes exceeds query pool max " + std::to_string(pool_ceiling) +
            " bytes (pool='query')");
  }
  if (budget_ != nullptr && !budget_->Unlimited() && pool_ceiling == 0) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "query memory pool max is zero bytes");
  }

  const auto start = std::chrono::steady_clock::now();
  const uint64_t timeout = wait_timeout_ms.value_or(WaitTimeoutMsFromEnv());
  const auto deadline = start + std::chrono::milliseconds(timeout);

  auto try_once = [&](size_t* granted_out, Status* err_out) -> bool {
    if (budget_ == nullptr) {
      *granted_out = target_max;
      return true;
    }
    const size_t capped_max =
        budget_->Unlimited() ? target_max : std::min(target_max, pool_ceiling);
    // First attempt full preferred size (with cross-pool reclaim).
    if (budget_->TryReserveWithReclaim(MemoryPoolKind::kQuery, capped_max,
                                       err_out)) {
      *granted_out = capped_max;
      return true;
    }
    // If bounded below `capped_max`, try elastic shrink down to `floor_min`.
    // Start no lower than floor_min: if floor_min > capped_max/2 a plain
    // halving would skip the entire [floor_min, capped_max) window.
    size_t probe = std::max(capped_max / 2, floor_min);
    while (probe >= std::max<size_t>(1, floor_min)) {
      if (budget_->TryReserveWithReclaim(MemoryPoolKind::kQuery, probe,
                                         err_out)) {
        *granted_out = probe;
        return true;
      }
      if (probe == floor_min || probe <= 1) {
        break;
      }
      probe = std::max(floor_min, probe / 2);
    }
    return false;
  };

  size_t granted = 0;
  Status last_err;
  if (try_once(&granted, &last_err)) {
    const auto waited = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
    return QueryMemoryGrant(
        budget_, granted, /*limited=*/true,
        static_cast<uint64_t>(std::max<int64_t>(0, waited)));
  }

  std::unique_lock<std::mutex> lk(mu_);
  const auto sig = signal_;  // keep the wake-up target alive across the wait
  while (true) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      break;
    }
    const auto slice = std::min<std::chrono::milliseconds>(
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now) +
            std::chrono::milliseconds(1),
        std::chrono::milliseconds(10));
    sig->cv.wait_for(lk, slice);
    lk.unlock();
    if (try_once(&granted, &last_err)) {
      const auto waited = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::steady_clock::now() - start)
                              .count();
      return QueryMemoryGrant(
          budget_, granted, /*limited=*/true,
          static_cast<uint64_t>(std::max<int64_t>(0, waited)));
    }
    lk.lock();
  }

  const auto waited = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();
  return StatusError(
      StatusCode::kMemoryLimitExceeded,
      "timed out after " +
          std::to_string(std::max<int64_t>(0, waited) / 1'000'000) +
          "ms waiting for memory grant of at least " +
          std::to_string(floor_min > 0 ? floor_min : target_max) +
          " bytes from pool 'query' (pool_max=" + std::to_string(pool_ceiling) +
          ", soft_limit=" +
          std::to_string(budget_ != nullptr ? budget_->SoftLimit() : 0) + ")");
}

}  // namespace tinylamb
