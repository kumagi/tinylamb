/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "common/memory_budget.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>

#include "common/platform/process.hpp"

namespace tinylamb {

namespace {

thread_local bool tl_in_reclaim_hook = false;

size_t ReadEnvBytes(const char* name, size_t fallback) noexcept {
  const char* raw = std::getenv(name);
  if (raw == nullptr || raw[0] == '\0') {
    return fallback;
  }
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(raw, &end, 10);
  if (end == raw || *end != '\0') {
    return fallback;
  }
  return static_cast<size_t>(parsed);
}

bool ReadEnvBool(const char* name) noexcept {
  const char* raw = std::getenv(name);
  return raw != nullptr && raw[0] != '\0' && std::string_view(raw) != "0";
}

std::mutex g_temp_dir_mu;
std::filesystem::path g_default_temp_dir;
thread_local OocQueryScope* tl_ooc_query_scope = nullptr;

template <typename Fn>
void ForEachActiveQueryMetrics(Fn&& fn) noexcept {
  OocQueryMetrics* last = nullptr;
  for (OocQueryScope* s = tl_ooc_query_scope; s != nullptr;
       s = s->PrevScope()) {
    OocQueryMetrics* m = s->Metrics();
    if (m != nullptr && m != last) {
      fn(*m);
      last = m;
    }
  }
}

template <typename T>
void UpdateMaxAtomic(std::atomic<T>& target, T val) noexcept {
  T cur = target.load(std::memory_order_relaxed);
  while (val > cur &&
         !target.compare_exchange_weak(cur, val, std::memory_order_relaxed,
                                       std::memory_order_relaxed)) {
  }
}

}  // namespace

void OocQueryMetrics::RecordOperatorPeakBytes(size_t bytes) noexcept {
  // Hot path: OOC sinks report their monotonically growing resident bytes
  // once per input row. A relaxed load keeps that a plain shared read and
  // only pays for the CAS when a new peak is actually reached.
  if (bytes <= op_peak_bytes.load(std::memory_order_relaxed)) {
    return;
  }
  UpdateMaxAtomic(op_peak_bytes, bytes);
}

void OocQueryMetrics::RecordSpillBytes(size_t bytes) noexcept {
  spill_ops.fetch_add(1, std::memory_order_relaxed);
  if (bytes > 0) {
    spill_bytes.fetch_add(bytes, std::memory_order_relaxed);
  }
}

void OocQueryMetrics::RecordPartitionDepth(uint32_t depth) noexcept {
  UpdateMaxAtomic(max_partition_depth, depth);
}

void OocQueryMetrics::RecordSkewFallback() noexcept {
  skew_fallbacks.fetch_add(1, std::memory_order_relaxed);
}

void OocQueryMetrics::RecordReclaim(size_t bytes) noexcept {
  reclaim_calls.fetch_add(1, std::memory_order_relaxed);
  if (bytes > 0) {
    reclaim_bytes.fetch_add(bytes, std::memory_order_relaxed);
  }
}

OocTelemetrySnapshot OocQueryMetrics::Snapshot() const noexcept {
  return OocTelemetrySnapshot{
      .op_peak_bytes = op_peak_bytes.load(std::memory_order_acquire),
      .spill_bytes = spill_bytes.load(std::memory_order_acquire),
      .spill_ops = spill_ops.load(std::memory_order_acquire),
      .max_partition_depth =
          max_partition_depth.load(std::memory_order_acquire),
      .skew_fallbacks = skew_fallbacks.load(std::memory_order_acquire),
      .reclaim_calls = reclaim_calls.load(std::memory_order_acquire),
      .reclaim_bytes = reclaim_bytes.load(std::memory_order_acquire),
  };
}

OocQueryScope::OocQueryScope(OocQueryMetrics* metrics) noexcept
    : metrics_(metrics), prev_scope_(tl_ooc_query_scope) {
  tl_ooc_query_scope = this;
}

OocQueryScope::~OocQueryScope() { tl_ooc_query_scope = prev_scope_; }

OocQueryScope* OocQueryScope::Top() noexcept { return tl_ooc_query_scope; }

OocQueryMetrics* OocQueryScope::Current() noexcept {
  return tl_ooc_query_scope != nullptr ? tl_ooc_query_scope->Metrics()
                                       : nullptr;
}

ProcessMemoryBudget::ProcessMemoryBudget() {
  // `TINYLAMB_MEMORY_LIMIT_BYTES=auto` opts into scaling with the host: half
  // of physical memory (never below the 16 GiB default).  The storage and
  // columnar pools then scale with the limit instead of keeping their fixed
  // 4 GiB / 8 GiB caps, so a large machine can keep a multi-GiB working set
  // of column projections resident.  Any other value keeps the fixed caps.
  size_t default_limit = kDefaultLimitBytes;
  bool auto_scaled = false;
  if (const char* env = std::getenv("TINYLAMB_MEMORY_LIMIT_BYTES");
      env != nullptr && std::string_view(env) == "auto") {
    if (const uint64_t phys = platform::PhysicalMemoryBytes(); phys > 0) {
      default_limit =
          std::max<size_t>(kDefaultLimitBytes, static_cast<size_t>(phys / 2));
      auto_scaled = default_limit > kDefaultLimitBytes;
    }
  }
  const size_t lim = ReadEnvBytes("TINYLAMB_MEMORY_LIMIT_BYTES", default_limit);
  limit_.store(lim, std::memory_order_relaxed);
  const size_t default_storage =
      lim == 0 ? (size_t{4} << 30)
               : (auto_scaled ? lim / 4
                              : std::min<size_t>(size_t{4} << 30, lim / 4));
  // Let reusable column projections use the shared headroom. Reservation
  // still enforces the process soft limit and every other pool's floor; a
  // fixed 2 GiB cap otherwise rebuilds projections on every large scan.
  const size_t default_columnar =
      lim == 0 ? (size_t{8} << 30)
               : (auto_scaled ? lim / 2
                              : std::min<size_t>(size_t{8} << 30, lim / 2));
  storage_limit_.store(
      ReadEnvBytes("TINYLAMB_BUFFER_POOL_BYTES", default_storage),
      std::memory_order_relaxed);
  columnar_limit_.store(
      ReadEnvBytes("TINYLAMB_COLUMNAR_CACHE_BYTES", default_columnar),
      std::memory_order_relaxed);
  const size_t soft = lim == 0 ? 0 : (lim / 5) * 4;
  size_t s_pct = ReadEnvBytes("TINYLAMB_STORAGE_FLOOR_PCT", 25);
  size_t q_pct = ReadEnvBytes("TINYLAMB_QUERY_FLOOR_PCT", 25);
  size_t t_pct = ReadEnvBytes("TINYLAMB_TX_FLOOR_PCT", 5);
  size_t c_pct = ReadEnvBytes("TINYLAMB_COLUMNAR_FLOOR_PCT", 0);
  if (s_pct + q_pct + t_pct + c_pct > 100) {
    s_pct = 25;
    q_pct = 25;
    t_pct = 5;
    c_pct = 0;
  }
  storage_floor_.store((soft / 100) * s_pct, std::memory_order_relaxed);
  query_floor_.store((soft / 100) * q_pct, std::memory_order_relaxed);
  tx_floor_.store((soft / 100) * t_pct, std::memory_order_relaxed);
  columnar_floor_.store((soft / 100) * c_pct, std::memory_order_relaxed);
  emergency_limit_.store(lim == 0
                             ? (size_t{64} << 20)
                             : std::min<size_t>(size_t{64} << 20, lim / 20),
                         std::memory_order_relaxed);
  op_memory_override_bytes_.store(ReadEnvBytes("TINYLAMB_OP_MEMORY_BYTES", 0),
                                  std::memory_order_relaxed);
  always_spill_enabled_.store(ReadEnvBool("TINYLAMB_ALWAYS_SPILL"),
                              std::memory_order_relaxed);
  temp_limit_bytes_.store(ReadEnvBytes("TINYLAMB_TEMP_LIMIT_BYTES", 0),
                          std::memory_order_relaxed);
  inject_memory_fail_countdown_.store(
      ReadEnvBytes("TINYLAMB_INJECT_MEMORY_FAIL", 0),
      std::memory_order_relaxed);
  inject_spill_io_fail_countdown_.store(
      ReadEnvBytes("TINYLAMB_INJECT_SPILL_IO_FAIL", 0),
      std::memory_order_relaxed);
}

ProcessMemoryBudget& ProcessMemoryBudget::Global() {
  static ProcessMemoryBudget instance;
  return instance;
}

void ProcessMemoryBudget::UpdatePeak(std::atomic<size_t>& peak,
                                     size_t val) noexcept {
  size_t cur = peak.load(std::memory_order_relaxed);
  while (val > cur &&
         !peak.compare_exchange_weak(cur, val, std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
  }
}

std::atomic<size_t>& ProcessMemoryBudget::PoolCounter(
    MemoryPoolKind pool) noexcept {
  switch (pool) {
    case MemoryPoolKind::kStorage:
      return storage_used_;
    case MemoryPoolKind::kColumnar:
      return columnar_used_;
    case MemoryPoolKind::kTx:
      return tx_used_;
    case MemoryPoolKind::kQuery:
    default:
      return query_used_;
  }
}

const std::atomic<size_t>& ProcessMemoryBudget::PoolCounter(
    MemoryPoolKind pool) const noexcept {
  switch (pool) {
    case MemoryPoolKind::kStorage:
      return storage_used_;
    case MemoryPoolKind::kColumnar:
      return columnar_used_;
    case MemoryPoolKind::kTx:
      return tx_used_;
    case MemoryPoolKind::kQuery:
    default:
      return query_used_;
  }
}

std::atomic<size_t>& ProcessMemoryBudget::PoolPeakCounter(
    MemoryPoolKind pool) noexcept {
  switch (pool) {
    case MemoryPoolKind::kStorage:
      return storage_peak_;
    case MemoryPoolKind::kColumnar:
      return columnar_peak_;
    case MemoryPoolKind::kTx:
      return tx_peak_;
    case MemoryPoolKind::kQuery:
    default:
      return query_peak_;
  }
}

size_t ProcessMemoryBudget::StoragePoolLimit() const noexcept {
  return storage_limit_.load(std::memory_order_acquire);
}

size_t ProcessMemoryBudget::ColumnarPoolLimit() const noexcept {
  return columnar_limit_.load(std::memory_order_acquire);
}

size_t ProcessMemoryBudget::PoolFloorBytes(MemoryPoolKind pool) const noexcept {
  switch (pool) {
    case MemoryPoolKind::kStorage:
      return storage_floor_.load(std::memory_order_acquire);
    case MemoryPoolKind::kColumnar:
      return columnar_floor_.load(std::memory_order_acquire);
    case MemoryPoolKind::kTx: {
      const size_t base = tx_floor_.load(std::memory_order_acquire);
      const size_t srv = server_tx_floor_.load(std::memory_order_acquire);
      const size_t lim = limit_.load(std::memory_order_acquire);
      if (lim == 0) {
        return base + srv;
      }
      const size_t soft = (lim / 5) * 4;
      const size_t other = storage_floor_.load(std::memory_order_acquire) +
                           columnar_floor_.load(std::memory_order_acquire) +
                           query_floor_.load(std::memory_order_acquire);
      const size_t max_tx = soft > other ? soft - other : base;
      return std::min(base + srv, std::max(base, max_tx));
    }
    case MemoryPoolKind::kQuery:
    default:
      return query_floor_.load(std::memory_order_acquire);
  }
}

size_t ProcessMemoryBudget::ProtectedFloorForOthers(
    MemoryPoolKind pool) const noexcept {
  size_t reserved_for_others = 0;
  for (MemoryPoolKind k : {MemoryPoolKind::kStorage, MemoryPoolKind::kColumnar,
                           MemoryPoolKind::kQuery, MemoryPoolKind::kTx}) {
    if (k == pool) {
      continue;
    }
    const size_t fl = PoolFloorBytes(k);
    if (fl == 0) {
      continue;
    }
    const size_t used = PoolCounter(k).load(std::memory_order_acquire);
    if (fl > used) {
      reserved_for_others += fl - used;
    }
  }
  return reserved_for_others;
}

size_t ProcessMemoryBudget::EffectivePoolMaxBytes(
    MemoryPoolKind pool) const noexcept {
  const size_t l = limit_.load(std::memory_order_acquire);
  const size_t pool_cap = PoolLimitFor(pool);
  if (l == 0) {
    return pool_cap == 0 ? std::numeric_limits<size_t>::max() : pool_cap;
  }
  const size_t soft = (l / 5) * 4;
  size_t other_floors = 0;
  for (MemoryPoolKind k : {MemoryPoolKind::kStorage, MemoryPoolKind::kColumnar,
                           MemoryPoolKind::kQuery, MemoryPoolKind::kTx}) {
    if (k != pool) {
      other_floors += PoolFloorBytes(k);
    }
  }
  size_t max_bytes = soft > other_floors ? soft - other_floors : 0;
  if (pool_cap > 0) {
    max_bytes = std::min(max_bytes, pool_cap);
  }
  return max_bytes;
}

size_t ProcessMemoryBudget::PoolLimitFor(MemoryPoolKind pool) const noexcept {
  switch (pool) {
    case MemoryPoolKind::kStorage:
      return storage_limit_.load(std::memory_order_acquire);
    case MemoryPoolKind::kColumnar:
      return columnar_limit_.load(std::memory_order_acquire);
    case MemoryPoolKind::kTx:
    case MemoryPoolKind::kQuery:
    default:
      return 0;
  }
}

void ProcessMemoryBudget::RecordRssObservation(
    uint64_t rss_bytes) const noexcept {
  if (rss_bytes == 0) {
    return;
  }
  uint64_t cur_peak_rss = peak_rss_bytes_.load(std::memory_order_relaxed);
  while (rss_bytes > cur_peak_rss &&
         !peak_rss_bytes_.compare_exchange_weak(cur_peak_rss, rss_bytes,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
  }
  const uint64_t tracked = total_used_.load(std::memory_order_acquire);
  const uint64_t unaccounted = rss_bytes > tracked ? rss_bytes - tracked : 0;
  uint64_t cur_peak_unacc =
      peak_unaccounted_rss_bytes_.load(std::memory_order_relaxed);
  while (unaccounted > cur_peak_unacc &&
         !peak_unaccounted_rss_bytes_.compare_exchange_weak(
             cur_peak_unacc, unaccounted, std::memory_order_relaxed,
             std::memory_order_relaxed)) {
  }
}

uint64_t ProcessMemoryBudget::CurrentUnaccountedRssBytes() const noexcept {
  const uint64_t rss = platform::CurrentProcessRssBytes();
  RecordRssObservation(rss);
  const uint64_t tracked = total_used_.load(std::memory_order_acquire);
  return rss > tracked ? rss - tracked : 0;
}

void ProcessMemoryBudget::ResetPeaks() noexcept {
  storage_peak_.store(storage_used_.load(std::memory_order_acquire),
                      std::memory_order_release);
  columnar_peak_.store(columnar_used_.load(std::memory_order_acquire),
                       std::memory_order_release);
  query_peak_.store(query_used_.load(std::memory_order_acquire),
                    std::memory_order_release);
  tx_peak_.store(tx_used_.load(std::memory_order_acquire),
                 std::memory_order_release);
  total_peak_.store(total_used_.load(std::memory_order_acquire),
                    std::memory_order_release);
  peak_rss_bytes_.store(0, std::memory_order_release);
  peak_unaccounted_rss_bytes_.store(0, std::memory_order_release);
}

void ProcessMemoryBudget::RecordOperatorPeakBytes(size_t bytes) noexcept {
  // Called from per-row OOC sink hot paths. telemetry_ is one process-global
  // cache line: avoid the CAS unless the global peak actually moves. Query
  // metrics are still notified on every call (they early-out themselves).
  if (bytes > telemetry_.op_peak_bytes.load(std::memory_order_relaxed)) {
    telemetry_.RecordOperatorPeakBytes(bytes);
  }
  ForEachActiveQueryMetrics(
      [&](OocQueryMetrics& m) { m.RecordOperatorPeakBytes(bytes); });
}

void ProcessMemoryBudget::RecordSpillBytes(size_t bytes) noexcept {
  telemetry_.RecordSpillBytes(bytes);
  ForEachActiveQueryMetrics(
      [&](OocQueryMetrics& m) { m.RecordSpillBytes(bytes); });
}

void ProcessMemoryBudget::RecordPartitionDepth(uint32_t depth) noexcept {
  telemetry_.RecordPartitionDepth(depth);
  ForEachActiveQueryMetrics(
      [&](OocQueryMetrics& m) { m.RecordPartitionDepth(depth); });
}

void ProcessMemoryBudget::RecordSkewFallback() noexcept {
  telemetry_.RecordSkewFallback();
  ForEachActiveQueryMetrics([](OocQueryMetrics& m) { m.RecordSkewFallback(); });
}

void ProcessMemoryBudget::RecordReclaim(size_t bytes) noexcept {
  telemetry_.RecordReclaim(bytes);
  ForEachActiveQueryMetrics(
      [&](OocQueryMetrics& m) { m.RecordReclaim(bytes); });
}

OocTelemetrySnapshot ProcessMemoryBudget::TelemetrySnapshot() const noexcept {
  return telemetry_.Snapshot();
}

uint64_t ProcessMemoryBudget::SampledRssBytes(
    bool force_refresh) const noexcept {
  if (!check_rss_pressure_.load(std::memory_order_acquire)) {
    return 0;
  }
  const uint64_t tick =
      rss_sample_tick_.fetch_add(1, std::memory_order_relaxed);
  const int64_t now_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  const int64_t prev_ns = last_rss_ns_.load(std::memory_order_relaxed);
  if (force_refresh || last_rss_bytes_.load(std::memory_order_relaxed) == 0 ||
      (tick & 255ULL) == 0 || (now_ns - prev_ns) >= 2'000'000LL) {
    last_rss_ns_.store(now_ns, std::memory_order_relaxed);
    const uint64_t rss = platform::CurrentProcessRssBytes();
    if (rss > 0) {
      last_rss_bytes_.store(rss, std::memory_order_release);
      RecordRssObservation(rss);
    }
  }
  return last_rss_bytes_.load(std::memory_order_acquire);
}

size_t ProcessMemoryBudget::Available() const noexcept {
  const size_t l = limit_.load(std::memory_order_acquire);
  if (l == 0) {
    return std::numeric_limits<size_t>::max();
  }
  const size_t u = total_used_.load(std::memory_order_acquire);
  size_t avail = u < l ? l - u : 0;
  if (check_rss_pressure_.load(std::memory_order_acquire)) {
    const uint64_t rss = SampledRssBytes(false);
    // Reserve ~12.5% headroom for allocator metadata, thread stacks, and
    // unbudgeted containers so RSS stays safely below Limit().
    const size_t rss_cap = (l / 8) * 7;
    if (rss >= rss_cap) {
      return 0;
    }
    const auto rss_avail = static_cast<size_t>(rss_cap - rss);
    avail = std::min(avail, rss_avail);
  }
  return avail;
}

bool ProcessMemoryBudget::UnderMemoryPressure() const noexcept {
  const size_t l = limit_.load(std::memory_order_acquire);
  if (l == 0) {
    return false;
  }
  const size_t u = total_used_.load(std::memory_order_acquire);
  if (u >= SoftLimit()) {
    return true;
  }
  if (check_rss_pressure_.load(std::memory_order_acquire)) {
    const uint64_t rss = SampledRssBytes(false);
    return rss >= SoftLimit();
  }
  return false;
}

bool ProcessMemoryBudget::CanReserve(MemoryPoolKind pool,
                                     size_t bytes) const noexcept {
  if (bytes == 0) {
    return true;
  }
  const size_t pool_cap = PoolLimitFor(pool);
  if (pool_cap > 0) {
    const size_t pu = PoolCounter(pool).load(std::memory_order_acquire);
    if (pu > pool_cap || bytes > pool_cap - pu) {
      return false;
    }
  }
  const size_t l = limit_.load(std::memory_order_acquire);
  if (l == 0) {
    return true;
  }
  const size_t soft = (l / 5) * 4;
  const size_t u = total_used_.load(std::memory_order_acquire);
  const size_t protected_other = ProtectedFloorForOthers(pool);
  if (u > soft || bytes > soft - u ||
      protected_other > soft - u - bytes) {
    return false;
  }
  if (check_rss_pressure_.load(std::memory_order_acquire)) {
    const uint64_t rss = SampledRssBytes(false);
    const size_t rss_soft = (l / 8) * 7;
    if (rss >= rss_soft) {
      return false;
    }
    // Check live headroom when the request itself could breach the RSS cap.
    if (bytes > (rss_soft - static_cast<size_t>(rss))) {
      return false;
    }
  }
  return true;
}

bool ProcessMemoryBudget::TryReserveNoFault(MemoryPoolKind pool, size_t bytes,
                                            bool respect_soft_limit) noexcept {
  if (bytes == 0) {
    return true;
  }
  const size_t pool_cap = PoolLimitFor(pool);
  const size_t l = limit_.load(std::memory_order_acquire);
  if (l > 0 && check_rss_pressure_.load(std::memory_order_acquire)) {
    const size_t rss_cap = respect_soft_limit ? (l / 8) * 7 : (l / 16) * 15;
    const uint64_t rss = SampledRssBytes(bytes >= (size_t{16} << 20));
    if (rss >= rss_cap || bytes > (rss_cap - static_cast<size_t>(rss))) {
      return false;
    }
  }
  const size_t effective_total_cap =
      l == 0 ? 0 : (respect_soft_limit ? ((l / 5) * 4) : l);
  size_t cur_total = total_used_.load(std::memory_order_acquire);
  while (true) {
    const size_t protected_other =
        respect_soft_limit ? ProtectedFloorForOthers(pool) : 0;
    if (effective_total_cap > 0 &&
        (cur_total > effective_total_cap ||
         bytes > effective_total_cap - cur_total ||
         protected_other > effective_total_cap - cur_total - bytes)) {
      return false;
    }
    if (total_used_.compare_exchange_weak(cur_total, cur_total + bytes,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
      break;
    }
  }
  if (pool_cap > 0) {
    auto& pc = PoolCounter(pool);
    size_t cur_pool = pc.load(std::memory_order_acquire);
    while (true) {
      if (cur_pool > pool_cap || bytes > pool_cap - cur_pool) {
        total_used_.fetch_sub(bytes, std::memory_order_acq_rel);
        return false;
      }
      if (pc.compare_exchange_weak(cur_pool, cur_pool + bytes,
                                   std::memory_order_acq_rel,
                                   std::memory_order_acquire)) {
        UpdatePeak(PoolPeakCounter(pool), cur_pool + bytes);
        break;
      }
    }
  } else {
    const size_t next_pool =
        PoolCounter(pool).fetch_add(bytes, std::memory_order_acq_rel) + bytes;
    UpdatePeak(PoolPeakCounter(pool), next_pool);
  }
  UpdatePeak(total_peak_, cur_total + bytes);
  return true;
}

bool ProcessMemoryBudget::TryReserve(MemoryPoolKind pool, size_t bytes,
                                     bool respect_soft_limit) noexcept {
  if (bytes > 0 && ConsumeInjectedMemoryFailure()) {
    return false;
  }
  return TryReserveNoFault(pool, bytes, respect_soft_limit);
}

uint64_t ProcessMemoryBudget::RegisterReclaimHook(ReclaimStage stage,
                                                  ReclaimCallback cb,
                                                  ReclaimRecency recency) {
  std::scoped_lock lk(hooks_mu_);
  const uint64_t id = next_hook_id_++;
  reclaim_hooks_.push_back(ReclaimHookEntry{.id = id,
                                            .stage = stage,
                                            .cb = std::move(cb),
                                            .recency = std::move(recency)});
  return id;
}

void ProcessMemoryBudget::UnregisterReclaimHook(uint64_t hook_id) {
  if (hook_id == 0) {
    return;
  }
  std::scoped_lock lk(hooks_mu_);
  std::erase_if(reclaim_hooks_, [hook_id](const ReclaimHookEntry& e) {
    return e.id == hook_id;
  });
}

uint64_t ProcessMemoryBudget::RegisterReleaseListener(
    ReleaseNotifyCallback cb) {
  std::scoped_lock lk(hooks_mu_);
  const uint64_t id = next_hook_id_++;
  release_listeners_.push_back(
      ReleaseListenerEntry{.id = id, .cb = std::move(cb)});
  PublishReleaseListenerView();
  return id;
}

void ProcessMemoryBudget::UnregisterReleaseListener(uint64_t listener_id) {
  if (listener_id == 0) {
    return;
  }
  std::scoped_lock lk(hooks_mu_);
  if (std::erase_if(release_listeners_,
                    [listener_id](const ReleaseListenerEntry& e) {
                      return e.id == listener_id;
                    }) == 0) {
    return;
  }
  PublishReleaseListenerView();
}

void ProcessMemoryBudget::PublishReleaseListenerView() {
  auto snapshot = std::make_shared<std::vector<ReleaseListenerEntry>>();
  snapshot->reserve(release_listeners_.size());
  for (const ReleaseListenerEntry& e : release_listeners_) {
    if (e.cb) {
      snapshot->push_back(e);
    }
  }
  // Publish the view first, then open the fast-path gate, so a reader that
  // sees count > 0 always loads a view at least as new as that count.
  release_view_.store(std::move(snapshot), std::memory_order_release);
  release_listener_count_.store(release_listeners_.size(),
                                std::memory_order_release);
}

size_t ProcessMemoryBudget::InvokeReclaimHooks(size_t target_bytes) noexcept {
  if (tl_in_reclaim_hook) {
    return 0;
  }
  tl_in_reclaim_hook = true;
  size_t reclaimed_total = 0;
  for (ReclaimStage stage :
       {ReclaimStage::kColumnar, ReclaimStage::kCleanStorage,
        ReclaimStage::kSpillableQuery}) {
    std::vector<std::pair<uint64_t, ReclaimCallback>> cbs;
    {
      std::unique_lock<std::mutex> lk(hooks_mu_, std::try_to_lock);
      if (!lk.owns_lock()) {
        continue;
      }
      for (const ReclaimHookEntry& e : reclaim_hooks_) {
        if (e.stage == stage && e.cb) {
          cbs.emplace_back(
              e.recency ? e.recency->load(std::memory_order_relaxed) : 0, e.cb);
        }
      }
    }
    std::stable_sort(cbs.begin(), cbs.end(), [](const auto& a, const auto& b) {
      return a.first < b.first;
    });
    for (const auto& [recency, cb] : cbs) {
      const size_t need =
          reclaimed_total < target_bytes ? target_bytes - reclaimed_total : 0;
      if (need == 0) {
        break;
      }
      try {
        reclaimed_total += cb(need);
      } catch (...) {
        // Reclaim hooks run inside memory-pressure paths marked noexcept;
        // count suppressed throws rather than crashing recovery paths.
        static std::atomic<uint64_t> suppressed_reclaim_throws{0};
        suppressed_reclaim_throws.fetch_add(1, std::memory_order_relaxed);
      }
    }
    if (reclaimed_total >= target_bytes) {
      break;
    }
  }
  tl_in_reclaim_hook = false;
  if (reclaimed_total > 0) {
    RecordReclaim(reclaimed_total);
  }
  return reclaimed_total;
}

void ProcessMemoryBudget::NotifyReleaseListeners() {
  // Hot path: Release() runs on every spill-chunk free across all workers.
  // Stay entirely off hooks_mu_ — read the published count, then the
  // copy-on-write snapshot; the callback list itself is immutable, so no
  // local vector copy or heap allocation is needed here.
  if (release_listener_count_.load(std::memory_order_acquire) == 0) {
    return;
  }
  const auto view = release_view_.load(std::memory_order_acquire);
  for (const ReleaseListenerEntry& e : *view) {
    try {
      e.cb();
    } catch (...) {
      // Waiter notifications best-effort; count rather than aborting other
      // listeners if a registered callback throws.
      static std::atomic<uint64_t> suppressed_listener_throws{0};
      suppressed_listener_throws.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

bool ProcessMemoryBudget::TryReserveWithReclaim(MemoryPoolKind pool,
                                                size_t bytes,
                                                Status* status_out) {
  if (TryReserve(pool, bytes, /*respect_soft_limit=*/true)) {
    return true;
  }
  if (!tl_in_reclaim_hook) {
    (void)InvokeReclaimHooks(bytes);
    if (TryReserve(pool, bytes, /*respect_soft_limit=*/true)) {
      return true;
    }
    // RSS-gated refusals count memory the allocator still retains after
    // free().  Hand it back to the OS and re-sample before giving up, or a
    // retaining allocator can starve every later reservation indefinitely.
    if (check_rss_pressure_.load(std::memory_order_acquire) &&
        platform::ReleaseFreeHeapToOs()) {
      (void)SampledRssBytes(/*force_refresh=*/true);
    }
    if (TryReserve(pool, bytes, /*respect_soft_limit=*/true)) {
      return true;
    }
  }
  if (status_out != nullptr) {
    *status_out = StatusError(
        StatusCode::kMemoryLimitExceeded,
        "memory limit exceeded reserving " + std::to_string(bytes) +
            " bytes from pool '" + std::string(MemoryPoolName(pool)) +
            "' (pool_max=" + std::to_string(EffectivePoolMaxBytes(pool)) +
            ", total_soft_limit=" + std::to_string(SoftLimit()) + ")");
  }
  return false;
}

void ProcessMemoryBudget::ReserveEmergency(MemoryPoolKind pool,
                                           size_t bytes) noexcept {
  if (bytes == 0) {
    return;
  }
  emergency_used_.fetch_add(bytes, std::memory_order_acq_rel);
  const size_t next_pool =
      PoolCounter(pool).fetch_add(bytes, std::memory_order_acq_rel) + bytes;
  UpdatePeak(PoolPeakCounter(pool), next_pool);
  const size_t next_total =
      total_used_.fetch_add(bytes, std::memory_order_acq_rel) + bytes;
  UpdatePeak(total_peak_, next_total);
}

void ProcessMemoryBudget::ReleaseEmergency(MemoryPoolKind pool,
                                           size_t bytes) noexcept {
  if (bytes == 0) {
    return;
  }
  size_t cur = emergency_used_.load(std::memory_order_acquire);
  size_t released = 0;
  while (cur != 0) {
    const size_t next = cur > bytes ? cur - bytes : 0;
    released = cur - next;
    if (emergency_used_.compare_exchange_weak(
            cur, next, std::memory_order_acq_rel, std::memory_order_acquire)) {
      break;
    }
  }
  // Only give back what the emergency ledger actually lost; releasing the
  // full argument when another thread already consumed part of it would
  // over-credit the pool and drift the budget.
  Release(pool, released);
}

void ProcessMemoryBudget::ReserveForced(MemoryPoolKind pool,
                                        size_t bytes) noexcept {
  if (bytes == 0) {
    return;
  }
  const size_t next_pool =
      PoolCounter(pool).fetch_add(bytes, std::memory_order_acq_rel) + bytes;
  UpdatePeak(PoolPeakCounter(pool), next_pool);
  const size_t next_total =
      total_used_.fetch_add(bytes, std::memory_order_acq_rel) + bytes;
  UpdatePeak(total_peak_, next_total);
}

void ProcessMemoryBudget::Release(MemoryPoolKind pool, size_t bytes) noexcept {
  if (bytes == 0) {
    return;
  }
  auto sub_saturating = [](std::atomic<size_t>& counter, size_t amount) {
    size_t cur = counter.load(std::memory_order_acquire);
    while (cur != 0) {
      const size_t next = cur > amount ? cur - amount : 0;
      if (counter.compare_exchange_weak(cur, next, std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
        return cur - next;
      }
    }
    return size_t{0};
  };
  const size_t pool_released = sub_saturating(PoolCounter(pool), bytes);
  if (pool_released > 0) {
    sub_saturating(total_used_, pool_released);
  }
  NotifyReleaseListeners();
}

void ProcessMemoryBudget::SetPoolFloorsForTest(size_t storage_floor,
                                               size_t columnar_floor,
                                               size_t query_floor,
                                               size_t tx_floor) noexcept {
  storage_floor_.store(storage_floor, std::memory_order_release);
  columnar_floor_.store(columnar_floor, std::memory_order_release);
  query_floor_.store(query_floor, std::memory_order_release);
  tx_floor_.store(tx_floor, std::memory_order_release);
}

void ProcessMemoryBudget::ResetForTest(size_t limit_bytes,
                                       size_t storage_limit_bytes,
                                       size_t columnar_limit_bytes,
                                       bool check_rss_pressure) noexcept {
  limit_.store(limit_bytes, std::memory_order_release);
  storage_limit_.store(storage_limit_bytes, std::memory_order_release);
  columnar_limit_.store(columnar_limit_bytes, std::memory_order_release);
  storage_floor_.store(0, std::memory_order_release);
  columnar_floor_.store(0, std::memory_order_release);
  query_floor_.store(0, std::memory_order_release);
  tx_floor_.store(0, std::memory_order_release);
  server_tx_floor_.store(0, std::memory_order_release);
  emergency_used_.store(0, std::memory_order_release);
  storage_used_.store(0, std::memory_order_release);
  columnar_used_.store(0, std::memory_order_release);
  query_used_.store(0, std::memory_order_release);
  tx_used_.store(0, std::memory_order_release);
  total_used_.store(0, std::memory_order_release);
  storage_peak_.store(0, std::memory_order_release);
  columnar_peak_.store(0, std::memory_order_release);
  query_peak_.store(0, std::memory_order_release);
  tx_peak_.store(0, std::memory_order_release);
  total_peak_.store(0, std::memory_order_release);
  check_rss_pressure_.store(check_rss_pressure, std::memory_order_release);
  last_rss_bytes_.store(0, std::memory_order_release);
  peak_rss_bytes_.store(0, std::memory_order_release);
  peak_unaccounted_rss_bytes_.store(0, std::memory_order_release);
  last_rss_ns_.store(0, std::memory_order_release);
}

bool ProcessMemoryBudget::ConsumeInjectedMemoryFailure() noexcept {
  uint64_t cur = inject_memory_fail_countdown_.load(std::memory_order_relaxed);
  while (cur > 0) {
    if (inject_memory_fail_countdown_.compare_exchange_weak(
            cur, cur - 1, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      return cur == 1;
    }
  }
  return false;
}

bool ProcessMemoryBudget::ConsumeInjectedSpillIoFailure() noexcept {
  uint64_t cur =
      inject_spill_io_fail_countdown_.load(std::memory_order_relaxed);
  while (cur > 0) {
    if (inject_spill_io_fail_countdown_.compare_exchange_weak(
            cur, cur - 1, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      return cur == 1;
    }
  }
  return false;
}

void ProcessMemoryBudget::SetStoragePoolLimitForTest(size_t bytes) noexcept {
  if (bytes == 0) {
    const size_t lim = limit_.load(std::memory_order_acquire);
    bytes = lim == 0 ? (size_t{4} << 30)
                     : std::min<size_t>(size_t{4} << 30, lim / 4);
  }
  storage_limit_.store(bytes, std::memory_order_release);
}

void ProcessMemoryBudget::SetColumnarPoolLimitForTest(size_t bytes) noexcept {
  if (bytes == 0) {
    const size_t lim = limit_.load(std::memory_order_acquire);
    bytes = lim == 0 ? (size_t{8} << 30)
                     : std::min<size_t>(size_t{8} << 30, lim / 2);
  }
  columnar_limit_.store(bytes, std::memory_order_release);
}

void ProcessMemoryBudget::ResetTestKnobs() noexcept {
  op_memory_override_bytes_.store(0, std::memory_order_release);
  always_spill_enabled_.store(false, std::memory_order_release);
  temp_limit_bytes_.store(0, std::memory_order_release);
  inject_memory_fail_countdown_.store(0, std::memory_order_release);
  inject_spill_io_fail_countdown_.store(0, std::memory_order_release);
}

void ProcessMemoryBudget::SetDefaultTempDirectory(
    const std::filesystem::path& dir) {
  std::scoped_lock lk(g_temp_dir_mu);
  g_default_temp_dir = dir;
}

std::filesystem::path ProcessMemoryBudget::ResolveTempDirectory() {
  if (const char* env = std::getenv("TINYLAMB_TEMP");
      env != nullptr && env[0] != '\0') {
    std::filesystem::path dir(env);
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    return dir;
  }
  {
    std::scoped_lock lk(g_temp_dir_mu);
    if (!g_default_temp_dir.empty()) {
      std::error_code ec;
      std::filesystem::create_directories(g_default_temp_dir, ec);
      return g_default_temp_dir;
    }
  }
  return std::filesystem::temp_directory_path();
}

ProcessMemoryCharge::ProcessMemoryCharge(MemoryPoolKind pool, size_t bytes,
                                         bool forced) noexcept
    : pool_(pool), bytes_(0), valid_(true) {
  if (bytes == 0) {
    return;
  }
  if (forced) {
    AddForced(bytes);
  } else {
    valid_ = TryAdd(bytes, /*respect_soft_limit=*/true);
  }
}

ProcessMemoryCharge& ProcessMemoryCharge::operator=(
    ProcessMemoryCharge&& other) noexcept {
  if (this != &other) {
    ReleaseAll();
    pool_ = other.pool_;
    bytes_ = other.bytes_;
    valid_ = other.valid_;
    other.bytes_ = 0;
    other.valid_ = false;
  }
  return *this;
}

bool ProcessMemoryCharge::TryAdd(size_t bytes,
                                 bool respect_soft_limit) noexcept {
  if (bytes == 0) {
    return true;
  }
  if (!ProcessMemoryBudget::Global().TryReserve(pool_, bytes,
                                                respect_soft_limit)) {
    return false;
  }
  bytes_ += bytes;
  return true;
}

bool ProcessMemoryCharge::TryAddWithReclaim(size_t bytes) {
  if (bytes == 0) {
    return true;
  }
  if (!ProcessMemoryBudget::Global().TryReserveWithReclaim(pool_, bytes)) {
    return false;
  }
  bytes_ += bytes;
  return true;
}

void ProcessMemoryCharge::AddForced(size_t bytes) noexcept {
  if (bytes == 0) {
    return;
  }
  ProcessMemoryBudget::Global().ReserveForced(pool_, bytes);
  bytes_ += bytes;
}

void ProcessMemoryCharge::Release(size_t bytes) noexcept {
  const size_t n = std::min(bytes, bytes_);
  if (n == 0) {
    return;
  }
  ProcessMemoryBudget::Global().Release(pool_, n);
  bytes_ -= n;
}

void ProcessMemoryCharge::ReleaseAll() noexcept {
  if (bytes_ > 0) {
    ProcessMemoryBudget::Global().Release(pool_, bytes_);
    bytes_ = 0;
  }
}

ScopedReclaimHook::ScopedReclaimHook(ReclaimStage stage, ReclaimCallback cb,
                                     ReclaimRecency recency)
    : id_(ProcessMemoryBudget::Global().RegisterReclaimHook(
          stage, std::move(cb), std::move(recency))) {}

void ScopedReclaimHook::Reset() {
  if (id_ != 0) {
    ProcessMemoryBudget::Global().UnregisterReclaimHook(id_);
    id_ = 0;
  }
}

}  // namespace tinylamb
