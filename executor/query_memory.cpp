/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/query_memory.hpp"

#include <atomic>
#include <cerrno>
#include <cstdlib>

#include "common/log_message.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {
// RAM-resident SF=10 (tpch Phase3-1): a full lineitem probe (~60M rows x
// ~100B) plus join/agg working copies fits in ~6-8 GiB, so the previous 1 GiB
// default spilled immediately on every large query. 8 GiB matches the value
// benchmark/tpch_benchmark.cpp sets via the environment variable; operators
// can still override or disable ("0") with TINYLAMB_QUERY_MEMORY_BYTES.
constexpr size_t kDefaultQueryMemoryBytes = size_t{8} << 30;  // 8 GiB
}

size_t QueryMemoryBudget::LimitFromEnv() {
  const char* env = std::getenv("TINYLAMB_QUERY_MEMORY_BYTES");
  if (env == nullptr || env[0] == '\0') {
    return kDefaultQueryMemoryBytes;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long long bytes = std::strtoull(env, &end, 10);
  if (errno != 0 || end == env || *end != '\0') {
    // Garbage in the environment variable must not silently disable the
    // budget ("abc" would parse as 0 == unlimited).
    LOG(WARN) << "Invalid TINYLAMB_QUERY_MEMORY_BYTES: " << env;
    return kDefaultQueryMemoryBytes;
  }
  return static_cast<size_t>(bytes);
}

QueryMemoryBudget::QueryMemoryBudget(size_t limit_bytes)
    : limit_(limit_bytes) {}

QueryMemoryBudget& QueryMemoryBudget::Global() {
  static QueryMemoryBudget instance(LimitFromEnv());
  return instance;
}

void QueryMemoryBudget::ResetForTest(size_t limit_bytes) {
  limit_ = limit_bytes;
  used_.store(0, std::memory_order_relaxed);
}

size_t QueryMemoryBudget::Remaining() const {
  if (Unlimited()) {
    return static_cast<size_t>(-1);
  }
  const size_t used = Used();
  return used >= limit_ ? 0 : limit_ - used;
}

bool QueryMemoryBudget::CanReserve(size_t bytes) const {
  if (bytes == 0 || Unlimited()) {
    return true;
  }
  const size_t soft =
      limit_ / kSoftFractionDen * kSoftFractionNum;  // 80% of limit
  const size_t cur = used_.load(std::memory_order_relaxed);
  return cur < soft && soft - cur >= bytes;
}

void QueryMemoryBudget::ReserveForced(size_t bytes) {
  if (bytes == 0) {
    return;
  }
  used_.fetch_add(bytes, std::memory_order_relaxed);
}

void QueryMemoryBudget::Release(size_t bytes) {
  if (bytes == 0) {
    return;
  }
  size_t cur = used_.load(std::memory_order_relaxed);
  for (;;) {
    const size_t next = cur > bytes ? cur - bytes : 0;
    if (used_.compare_exchange_weak(cur, next, std::memory_order_relaxed)) {
      return;
    }
  }
}

size_t EstimateValueBytes(const Value& value) {
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
    case ValueType::kArray: {
      size_t bytes = kBase + value.ArrayElementSqlType().size();
      for (const Value& element : value.ArrayElements()) {
        bytes += EstimateValueBytes(element);
      }
      return bytes;
    }
    default:
      return kBase + 16;
  }
}

size_t EstimateRowBytes(const Row& row) {
  size_t total = 64;  // vector / Row overhead
  for (const Value& value : row.values_) {
    total += EstimateValueBytes(value);
  }
  return total;
}

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
  QueryMemoryBudget::Global().ReserveForced(bytes);
  bytes_ += bytes;
}

void QueryMemoryCharge::ReleaseAll() {
  if (bytes_ == 0) {
    return;
  }
  QueryMemoryBudget::Global().Release(bytes_);
  bytes_ = 0;
}

}  // namespace tinylamb
