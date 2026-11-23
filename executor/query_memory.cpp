/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/query_memory.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <stdexcept>

#include "common/log_message.hpp"
#include "common/memory_budget.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {
constexpr size_t kDefaultQueryMemoryBytes = size_t{8} << 30;  // 8 GiB
}  // namespace

size_t QueryMemoryBudget::LimitFromEnv() {
  const size_t proc_lim = ProcessMemoryBudget::Global().Limit();
  const size_t default_bytes =
      proc_lim == 0 ? kDefaultQueryMemoryBytes
                    : std::min(kDefaultQueryMemoryBytes, proc_lim);
  const char* env = std::getenv("TINYLAMB_QUERY_MEMORY_BYTES");
  if (env == nullptr || env[0] == '\0') {
    return default_bytes;
  }
  // strtoull silently wraps a leading '-' (e.g. "-1" becomes ULLONG_MAX);
  // reject any sign or whitespace before the digits.
  if (env[0] == '-' || env[0] == '+' ||
      std::isspace(static_cast<unsigned char>(env[0])) != 0) {
    LOG(WARN) << "Invalid TINYLAMB_QUERY_MEMORY_BYTES: " << env;
    return default_bytes;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long long bytes = std::strtoull(env, &end, 10);
  if (errno != 0 || end == env || *end != '\0') {
    // Garbage in the environment variable must not silently disable the
    // budget ("abc" would parse as 0 == unlimited).
    LOG(WARN) << "Invalid TINYLAMB_QUERY_MEMORY_BYTES: " << env;
    return default_bytes;
  }
  const auto requested = static_cast<size_t>(bytes);
  if (requested == 0) {
    // 0 keeps the historical "disable the query budget" meaning, but must
    // not bypass the process-level ceiling (otherwise spill decisions stop
    // firing and the process can OOM).
    return proc_lim;
  }
  return proc_lim == 0 ? requested : std::min(requested, proc_lim);
}

QueryMemoryBudget::QueryMemoryBudget(size_t limit_bytes)
    : limit_(limit_bytes),
      soft_(limit_bytes / kSoftFractionDen * kSoftFractionNum) {}

QueryMemoryBudget& QueryMemoryBudget::Global() {
  static QueryMemoryBudget instance(LimitFromEnv());
  return instance;
}

void QueryMemoryBudget::ResetForTest(size_t limit_bytes,
                                     bool isolate_from_process) {
  const size_t prev = used_.exchange(0, std::memory_order_acq_rel);
  if (!test_isolated_ && prev > 0) {
    ProcessMemoryBudget::Global().Release(MemoryPoolKind::kQuery, prev);
  }
  limit_ = limit_bytes;
  soft_ = limit_bytes / kSoftFractionDen * kSoftFractionNum;
  test_isolated_ = isolate_from_process;
}

size_t QueryMemoryBudget::Remaining() const {
  if (Unlimited()) {
    return static_cast<size_t>(-1);
  }
  const size_t used = Used();
  const size_t local_rem = used >= limit_ ? 0 : limit_ - used;
  if (test_isolated_) {
    return local_rem;
  }
  return std::min(local_rem, ProcessMemoryBudget::Global().Available());
}

bool QueryMemoryBudget::CanReserve(size_t bytes) const {
  if (bytes == 0) {
    return true;
  }
  if (!Unlimited()) {
    const size_t cur = used_.load(std::memory_order_acquire);
    if (cur >= soft_ || soft_ - cur < bytes) {
      return false;
    }
  }
  if (!test_isolated_) {
    return ProcessMemoryBudget::Global().CanReserve(MemoryPoolKind::kQuery,
                                                    bytes);
  }
  return true;
}

bool QueryMemoryBudget::TryReserve(size_t bytes) {
  if (bytes == 0) {
    return true;
  }
  if (!test_isolated_) {
    if (!ProcessMemoryBudget::Global().TryReserveWithReclaim(
            MemoryPoolKind::kQuery, bytes)) {
      return false;
    }
  }
  size_t cur = used_.load(std::memory_order_acquire);
  while (true) {
    if (!Unlimited() && (cur >= soft_ || soft_ - cur < bytes)) {
      if (!test_isolated_) {
        ProcessMemoryBudget::Global().Release(MemoryPoolKind::kQuery, bytes);
      }
      return false;
    }
    if (bytes > std::numeric_limits<size_t>::max() - cur) {
      if (!test_isolated_) {
        ProcessMemoryBudget::Global().Release(MemoryPoolKind::kQuery, bytes);
      }
      return false;
    }
    if (used_.compare_exchange_weak(cur, cur + bytes, std::memory_order_acq_rel,
                                    std::memory_order_acquire)) {
      return true;
    }
  }
}

void QueryMemoryBudget::ReserveForced(size_t bytes) {
  if (bytes == 0) {
    return;
  }
  used_.fetch_add(bytes, std::memory_order_acq_rel);
  if (!test_isolated_) {
    ProcessMemoryBudget::Global().ReserveForced(MemoryPoolKind::kQuery, bytes);
  }
}

void QueryMemoryBudget::Release(size_t bytes) {
  if (bytes == 0) {
    return;
  }
  size_t cur = used_.load(std::memory_order_acquire);
  size_t released = 0;
  for (;;) {
    released = std::min(cur, bytes);
    if (used_.compare_exchange_weak(cur, cur - released,
                                    std::memory_order_acq_rel,
                                    std::memory_order_acquire)) {
      break;
    }
  }
  if (!test_isolated_ && released > 0) {
    ProcessMemoryBudget::Global().Release(MemoryPoolKind::kQuery, released);
  }
}

size_t EstimateValueBytes(const Value& value) {
  if (value.type != ValueType::kArray) {
    return EstimateScalarValueBytes(value);
  }
  size_t bytes = 32 + value.ArrayElementSqlType().size();
  for (const Value& element : value.ArrayElements()) {
    bytes += EstimateValueBytes(element);
  }
  return bytes;
}

size_t EstimateRowBytes(const Row& row) { return EstimateRowBytesFast(row); }

QueryMemoryCharge::QueryMemoryCharge(size_t bytes) {
  if (bytes != 0) {
    QueryMemoryBudget::Global().ReserveForced(bytes);
    bytes_ = bytes;
  }
}

QueryMemoryCharge::QueryMemoryCharge(QueryMemoryCharge&& other) noexcept
    : bytes_(other.bytes_) {
  other.bytes_ = 0;
}

QueryMemoryCharge& QueryMemoryCharge::operator=(
    QueryMemoryCharge&& other) noexcept {
  if (this != &other) {
    ReleaseAll();
    bytes_ = other.bytes_;
    other.bytes_ = 0;
  }
  return *this;
}

QueryMemoryCharge::~QueryMemoryCharge() { ReleaseAll(); }

void QueryMemoryCharge::Add(size_t bytes) {
  if (bytes == 0) {
    return;
  }
  if (bytes > std::numeric_limits<size_t>::max() - bytes_) {
    throw std::overflow_error("query memory charge size overflow");
  }
  QueryMemoryBudget::Global().ReserveForced(bytes);
  bytes_ += bytes;
}

bool QueryMemoryCharge::TryAdd(size_t bytes) {
  if (bytes == 0) {
    return true;
  }
  if (bytes > std::numeric_limits<size_t>::max() - bytes_ ||
      !QueryMemoryBudget::Global().TryReserve(bytes)) {
    return false;
  }
  bytes_ += bytes;
  return true;
}

void QueryMemoryCharge::Release(size_t bytes) {
  const size_t rel = std::min(bytes_, bytes);
  if (rel == 0) {
    return;
  }
  bytes_ -= rel;
  QueryMemoryBudget::Global().Release(rel);
}

void QueryMemoryCharge::ReleaseAll() {
  if (bytes_ == 0) {
    return;
  }
  QueryMemoryBudget::Global().Release(bytes_);
  bytes_ = 0;
}

}  // namespace tinylamb
