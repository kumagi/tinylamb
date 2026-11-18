/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_COMMON_MEMORY_BUDGET_HPP
#define TINYLAMB_COMMON_MEMORY_BUDGET_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

#include "common/constants.hpp"

namespace tinylamb {

enum class MemoryPoolKind : uint8_t {
  kStorage = 0,
  kColumnar = 1,
  kQuery = 2,
  kTx = 3,
};

[[nodiscard]] constexpr std::string_view MemoryPoolName(
    MemoryPoolKind pool) noexcept {
  switch (pool) {
    case MemoryPoolKind::kStorage:
      return "storage";
    case MemoryPoolKind::kColumnar:
      return "columnar";
    case MemoryPoolKind::kQuery:
      return "query";
    case MemoryPoolKind::kTx:
      return "tx";
  }
  return "unknown";
}

// Reclaim order mandated by ADR 0003: columnar caches -> clean buffer pages
// -> reclaimable query operators. Hooks must use try_lock and never block.
enum class ReclaimStage : uint8_t {
  kColumnar = 0,
  kCleanStorage = 1,
  kSpillableQuery = 2,
};

// Optional monotonically increasing access stamp; lower values reclaim first
// within the same stage. The registry reads it without calling cache code.
using ReclaimRecency = std::shared_ptr<std::atomic<uint64_t>>;

using ReclaimCallback = std::function<size_t(size_t target_bytes)>;
using ReleaseNotifyCallback = std::function<void()>;

struct OocTelemetrySnapshot {
  size_t op_peak_bytes{0};
  uint64_t spill_bytes{0};
  uint64_t spill_ops{0};
  uint32_t max_partition_depth{0};
  uint64_t skew_fallbacks{0};
  uint64_t reclaim_calls{0};
  uint64_t reclaim_bytes{0};
};

// Thread-safe per-query OOC telemetry collector. Scoped queries attach an
// instance via OocQueryScope so EXPLAIN ANALYZE reports exact per-query OOC
// counters even when other queries run concurrently.
struct OocQueryMetrics {
  std::atomic<size_t> op_peak_bytes{0};
  std::atomic<uint64_t> spill_bytes{0};
  std::atomic<uint64_t> spill_ops{0};
  std::atomic<uint32_t> max_partition_depth{0};
  std::atomic<uint64_t> skew_fallbacks{0};
  std::atomic<uint64_t> reclaim_calls{0};
  std::atomic<uint64_t> reclaim_bytes{0};

  void RecordOperatorPeakBytes(size_t bytes) noexcept;
  void RecordSpillBytes(size_t bytes) noexcept;
  void RecordPartitionDepth(uint32_t depth) noexcept;
  void RecordSkewFallback() noexcept;
  void RecordReclaim(size_t bytes) noexcept;
  [[nodiscard]] OocTelemetrySnapshot Snapshot() const noexcept;
};

class OocQueryScope {
 public:
  explicit OocQueryScope(OocQueryMetrics* metrics) noexcept;
  OocQueryScope(const OocQueryScope&) = delete;
  OocQueryScope& operator=(const OocQueryScope&) = delete;
  OocQueryScope(OocQueryScope&&) = delete;
  OocQueryScope& operator=(OocQueryScope&&) = delete;
  ~OocQueryScope();

  [[nodiscard]] static OocQueryMetrics* Current() noexcept;
  [[nodiscard]] static OocQueryScope* Top() noexcept;
  [[nodiscard]] OocQueryMetrics* Metrics() const noexcept { return metrics_; }
  [[nodiscard]] OocQueryScope* PrevScope() const noexcept {
    return prev_scope_;
  }

 private:
  OocQueryMetrics* metrics_{nullptr};
  OocQueryScope* prev_scope_{nullptr};
};

// Coordinates process-wide tracked RAM across buffer pool storage, columnar
// caches, and query execution operators under a single hard/soft cap
// (configurable via TINYLAMB_MEMORY_LIMIT_BYTES, default 16 GiB).
class ProcessMemoryBudget {
 public:
  static constexpr size_t kDefaultLimitBytes = size_t{16} << 30;  // 16 GiB
  static ProcessMemoryBudget& Global();

  [[nodiscard]] bool Unlimited() const noexcept {
    return limit_.load(std::memory_order_acquire) == 0;
  }
  [[nodiscard]] size_t Limit() const noexcept {
    return limit_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t SoftLimit() const noexcept {
    const size_t l = limit_.load(std::memory_order_acquire);
    return l == 0 ? static_cast<size_t>(-1) : (l / 5) * 4;
  }
  [[nodiscard]] size_t RssWarnLimit() const noexcept {
    const size_t l = limit_.load(std::memory_order_acquire);
    return l == 0 ? static_cast<size_t>(-1) : (l / 8) * 7;
  }

  [[nodiscard]] size_t StorageUsed() const noexcept {
    return storage_used_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t ColumnarUsed() const noexcept {
    return columnar_used_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t QueryUsed() const noexcept {
    return query_used_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t TxUsed() const noexcept {
    return tx_used_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t TotalUsed() const noexcept {
    return total_used_.load(std::memory_order_acquire);
  }

  [[nodiscard]] size_t StoragePeak() const noexcept {
    return storage_peak_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t ColumnarPeak() const noexcept {
    return columnar_peak_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t QueryPeak() const noexcept {
    return query_peak_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t TxPeak() const noexcept {
    return tx_peak_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t TotalPeak() const noexcept {
    return total_peak_.load(std::memory_order_acquire);
  }
  [[nodiscard]] uint64_t PeakRssBytes() const noexcept {
    return peak_rss_bytes_.load(std::memory_order_acquire);
  }
  [[nodiscard]] uint64_t CurrentUnaccountedRssBytes() const noexcept;
  [[nodiscard]] uint64_t PeakUnaccountedRssBytes() const noexcept {
    return peak_unaccounted_rss_bytes_.load(std::memory_order_acquire);
  }
  void ResetPeaks() noexcept;

  // Global OOC telemetry helpers (also forwards to OocQueryScope::Current()).
  void RecordOperatorPeakBytes(size_t bytes) noexcept;
  void RecordSpillBytes(size_t bytes) noexcept;
  void RecordPartitionDepth(uint32_t depth) noexcept;
  void RecordSkewFallback() noexcept;
  void RecordReclaim(size_t bytes) noexcept;
  [[nodiscard]] OocTelemetrySnapshot TelemetrySnapshot() const noexcept;

  // Returns the configured target byte limit / guaranteed floor for a pool.
  [[nodiscard]] size_t StoragePoolLimit() const noexcept;
  [[nodiscard]] size_t ColumnarPoolLimit() const noexcept;
  [[nodiscard]] size_t PoolFloorBytes(MemoryPoolKind pool) const noexcept;
  // Registers fixed outbound socket-buffer reserve (`max_connections *
  // per_connection_buffer_bytes`) into the D1 `kTx` floor (ooca-v2.md §5.6).
  void AddServerBufferFloorBytes(size_t bytes) noexcept {
    server_tx_floor_.fetch_add(bytes, std::memory_order_release);
  }
  void SubServerBufferFloorBytes(size_t bytes) noexcept {
    size_t cur = server_tx_floor_.load(std::memory_order_acquire);
    while (!server_tx_floor_.compare_exchange_weak(
        cur, cur >= bytes ? cur - bytes : 0, std::memory_order_release,
        std::memory_order_acquire)) {
    }
  }
  [[nodiscard]] size_t ServerBufferFloorBytes() const noexcept {
    return server_tx_floor_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t EffectivePoolMaxBytes(
      MemoryPoolKind pool) const noexcept;
  [[nodiscard]] size_t EmergencyUsed() const noexcept {
    return emergency_used_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t EmergencyLimit() const noexcept {
    return emergency_limit_.load(std::memory_order_acquire);
  }

  // Approximate available headroom considering both tracked usage and sampled
  // OS RSS against Limit().
  [[nodiscard]] size_t Available() const noexcept;

  // True if `bytes` could be admitted into `pool` under the soft cap and live
  // OS RSS pressure.
  [[nodiscard]] bool CanReserve(MemoryPoolKind pool,
                                size_t bytes) const noexcept;
  [[nodiscard]] bool CanReserve(size_t bytes) const noexcept {
    return CanReserve(MemoryPoolKind::kQuery, bytes);
  }

  // Atomically reserves `bytes` against both total process headroom (up to
  // hard Limit() for query spills / soft limit depending on `respect_soft_limit`) and
  // the pool counter. Returns false without modifying counters when full.
  [[nodiscard]] bool TryReserve(MemoryPoolKind pool, size_t bytes,
                                bool respect_soft_limit = true) noexcept;
  // Accounting-only reservation: does NOT consume a fault-injection countdown.
  // Use for memory charges whose failure is absorbed by ReserveEmergency (so
  // injecting there would fire the probe with no observable effect).
  [[nodiscard]] bool TryReserveNoFault(MemoryPoolKind pool, size_t bytes,
                                       bool respect_soft_limit = true) noexcept;
  [[nodiscard]] bool TryReserve(size_t bytes) noexcept {
    return TryReserve(MemoryPoolKind::kQuery, bytes,
                      /*respect_soft_limit=*/true);
  }

  // Attempts TryReserve; if denied due to pressure, runs non-blocking reclaim
  // hooks in ADR 0003 order (kColumnar -> kCleanStorage -> kSpillableQuery)
  // and retries. Populate `status_out` with StatusCode::kMemoryLimitExceeded
  // (including requested bytes, limit, and pool name) on final rejection.
  [[nodiscard]] bool TryReserveWithReclaim(MemoryPoolKind pool, size_t bytes,
                                           Status* status_out = nullptr);

  // Small must-succeed emergency reserve (metadata, error paths, recovery).
  void ReserveEmergency(MemoryPoolKind pool, size_t bytes) noexcept;
  void ReleaseEmergency(MemoryPoolKind pool, size_t bytes) noexcept;

  // Non-blocking reclaim hook registration (ADR 0003). Hooks must not wait on
  // contended mutexes/latches (use try_lock).
  uint64_t RegisterReclaimHook(ReclaimStage stage, ReclaimCallback cb,
                               ReclaimRecency recency = {});
  void UnregisterReclaimHook(uint64_t hook_id);

  // Release listener registration (used by QueryMemoryCoordinator wakeup).
  uint64_t RegisterReleaseListener(ReleaseNotifyCallback cb);
  void UnregisterReleaseListener(uint64_t listener_id);

  void ReserveForced(MemoryPoolKind pool, size_t bytes) noexcept;
  void Release(MemoryPoolKind pool, size_t bytes) noexcept;

  // Overrides per-pool minimum floors for tests (ADR 0003).
  void SetPoolFloorsForTest(size_t storage_floor, size_t columnar_floor,
                            size_t query_floor, size_t tx_floor) noexcept;

  // Returns true when live tracked bytes or sampled OS RSS exceed the high
  // water mark (~80% of Limit()).
  [[nodiscard]] bool UnderMemoryPressure() const noexcept;

  // Refreshes and returns sampled process RSS in bytes (throttled across high
  // call frequency unless `force_refresh` is true).
  [[nodiscard]] uint64_t SampledRssBytes(
      bool force_refresh = false) const noexcept;

  // Overrides limits and zeroes pool usage counters for unit tests. If
  // `limit_bytes` == 0, process-level limits are disabled.
  void ResetForTest(size_t limit_bytes, size_t storage_limit_bytes = 0,
                    size_t columnar_limit_bytes = 0,
                    bool check_rss_pressure = false) noexcept;

  // Default directory for large temporary spill files when TINYLAMB_TEMP is
  // unset. Callers creating disk-backed DBs can register their DB directory so
  // large spills stay on the same filesystem instead of overflowing /tmp tmpfs.
  static void SetDefaultTempDirectory(const std::filesystem::path& dir);
  static std::filesystem::path ResolveTempDirectory();

  // P0 test / fault-injection knobs (env-initialized, programmatically
  // overridable in unit tests without restarting the process).
  [[nodiscard]] size_t OpMemoryOverrideBytes() const noexcept {
    return op_memory_override_bytes_.load(std::memory_order_acquire);
  }
  [[nodiscard]] bool AlwaysSpillEnabled() const noexcept {
    return always_spill_enabled_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t TempLimitBytes() const noexcept {
    return temp_limit_bytes_.load(std::memory_order_acquire);
  }
  [[nodiscard]] bool ConsumeInjectedMemoryFailure() noexcept;
  [[nodiscard]] bool ConsumeInjectedSpillIoFailure() noexcept;

  void SetTestOpMemoryBytes(size_t bytes) noexcept {
    op_memory_override_bytes_.store(bytes, std::memory_order_release);
  }
  void SetTestAlwaysSpill(bool enabled) noexcept {
    always_spill_enabled_.store(enabled, std::memory_order_release);
  }
  void SetTestTempLimitBytes(size_t bytes) noexcept {
    temp_limit_bytes_.store(bytes, std::memory_order_release);
  }
  // Overrides the storage / columnar pool caps without touching the usage
  // counters (unlike ResetForTest), so a fuzzer can shrink or grow the
  // buffer pool while databases are open and keep the cap across a
  // crash-reopen (recovery then runs under the same cap).  `0` restores the
  // documented default (`min(4 GiB, limit / 4)` / `min(2 GiB, limit / 8)`).
  void SetStoragePoolLimitForTest(size_t bytes) noexcept;
  void SetColumnarPoolLimitForTest(size_t bytes) noexcept;
  // If `countdown` > 0, the `countdown`-th reservation / spill I/O call
  // fails once (countdown == 0 disables injection).
  void SetTestMemoryFailCountdown(uint64_t countdown) noexcept {
    inject_memory_fail_countdown_.store(countdown, std::memory_order_release);
  }
  void SetTestSpillIoFailCountdown(uint64_t countdown) noexcept {
    inject_spill_io_fail_countdown_.store(countdown, std::memory_order_release);
  }
  void ResetTestKnobs() noexcept;

 private:
  ProcessMemoryBudget();

  static void UpdatePeak(std::atomic<size_t>& peak, size_t val) noexcept;
  void RecordRssObservation(uint64_t rss_bytes) const noexcept;
  std::atomic<size_t>& PoolCounter(MemoryPoolKind pool) noexcept;
  const std::atomic<size_t>& PoolCounter(MemoryPoolKind pool) const noexcept;
  std::atomic<size_t>& PoolPeakCounter(MemoryPoolKind pool) noexcept;
  [[nodiscard]] size_t PoolLimitFor(MemoryPoolKind pool) const noexcept;
  [[nodiscard]] size_t ProtectedFloorForOthers(
      MemoryPoolKind pool) const noexcept;
  size_t InvokeReclaimHooks(size_t target_bytes) noexcept;
  void NotifyReleaseListeners();
  // Rebuild the lock-free listener snapshot; caller must hold hooks_mu_.
  void PublishReleaseListenerView();

  struct ReclaimHookEntry {
    uint64_t id{0};
    ReclaimStage stage{ReclaimStage::kColumnar};
    ReclaimCallback cb;
    ReclaimRecency recency;
  };
  struct ReleaseListenerEntry {
    uint64_t id{0};
    ReleaseNotifyCallback cb;
  };

  std::atomic<size_t> limit_{kDefaultLimitBytes};
  std::atomic<size_t> storage_limit_{size_t{4} << 30};
  std::atomic<size_t> columnar_limit_{size_t{2} << 30};
  std::atomic<size_t> storage_floor_{0};
  std::atomic<size_t> columnar_floor_{0};
  std::atomic<size_t> query_floor_{0};
  std::atomic<size_t> tx_floor_{0};
  std::atomic<size_t> server_tx_floor_{0};
  std::atomic<size_t> emergency_limit_{size_t{64} << 20};
  std::atomic<size_t> emergency_used_{0};
  std::atomic<size_t> storage_used_{0};
  std::atomic<size_t> columnar_used_{0};
  std::atomic<size_t> query_used_{0};
  std::atomic<size_t> tx_used_{0};
  std::atomic<size_t> total_used_{0};
  std::atomic<size_t> storage_peak_{0};
  std::atomic<size_t> columnar_peak_{0};
  std::atomic<size_t> query_peak_{0};
  std::atomic<size_t> tx_peak_{0};
  std::atomic<size_t> total_peak_{0};
  std::atomic<bool> check_rss_pressure_{true};
  mutable std::atomic<uint64_t> last_rss_bytes_{0};
  mutable std::atomic<uint64_t> peak_rss_bytes_{0};
  mutable std::atomic<uint64_t> peak_unaccounted_rss_bytes_{0};
  mutable std::atomic<uint64_t> rss_sample_tick_{0};
  mutable std::atomic<int64_t> last_rss_ns_{0};

  mutable std::mutex hooks_mu_;
  uint64_t next_hook_id_{1};
  std::vector<ReclaimHookEntry> reclaim_hooks_;
  std::vector<ReleaseListenerEntry> release_listeners_;
  // Copy-on-write snapshot of the (filtered, non-null) release listeners so
  // Release() stays off hooks_mu_: an atomic counter short-circuits the empty
  // case and the shared_ptr load never contends with hook registration or
  // fails the way a try_lock did (dropped notifications cost waiters a full
  // poll slice).  Published only under hooks_mu_.
  std::atomic<uint64_t> release_listener_count_{0};
  std::atomic<std::shared_ptr<const std::vector<ReleaseListenerEntry>>>
      release_view_;

  std::atomic<size_t> op_memory_override_bytes_{0};
  std::atomic<bool> always_spill_enabled_{false};
  std::atomic<size_t> temp_limit_bytes_{0};
  std::atomic<uint64_t> inject_memory_fail_countdown_{0};
  std::atomic<uint64_t> inject_spill_io_fail_countdown_{0};
  OocQueryMetrics telemetry_{};
};

class ScopedReclaimHook {
 public:
  ScopedReclaimHook() = default;
  ScopedReclaimHook(ReclaimStage stage, ReclaimCallback cb,
                    ReclaimRecency recency = {});
  ScopedReclaimHook(const ScopedReclaimHook&) = delete;
  ScopedReclaimHook& operator=(const ScopedReclaimHook&) = delete;
  ScopedReclaimHook(ScopedReclaimHook&& other) noexcept : id_(other.id_) {
    other.id_ = 0;
  }
  ScopedReclaimHook& operator=(ScopedReclaimHook&& other) noexcept {
    if (this != &other) {
      Reset();
      id_ = other.id_;
      other.id_ = 0;
    }
    return *this;
  }
  ~ScopedReclaimHook() { Reset(); }

  void Reset();

 private:
  uint64_t id_{0};
};

class ProcessMemoryCharge {
 public:
  explicit ProcessMemoryCharge(MemoryPoolKind pool = MemoryPoolKind::kQuery,
                               size_t bytes = 0, bool forced = false) noexcept;
  ProcessMemoryCharge(const ProcessMemoryCharge&) = delete;
  ProcessMemoryCharge& operator=(const ProcessMemoryCharge&) = delete;
  ProcessMemoryCharge(ProcessMemoryCharge&& other) noexcept
      : pool_(other.pool_), bytes_(other.bytes_), valid_(other.valid_) {
    other.bytes_ = 0;
    other.valid_ = false;
  }
  ProcessMemoryCharge& operator=(ProcessMemoryCharge&& other) noexcept;
  ~ProcessMemoryCharge() { ReleaseAll(); }

  [[nodiscard]] bool TryAdd(size_t bytes,
                            bool respect_soft_limit = true) noexcept;
  [[nodiscard]] bool TryAddWithReclaim(size_t bytes);
  void AddForced(size_t bytes) noexcept;
  void Release(size_t bytes) noexcept;
  void ReleaseAll() noexcept;
  [[nodiscard]] size_t Bytes() const noexcept { return bytes_; }
  [[nodiscard]] bool Ok() const noexcept { return valid_; }

 private:
  MemoryPoolKind pool_;
  size_t bytes_{0};
  bool valid_{true};
};

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_MEMORY_BUDGET_HPP
