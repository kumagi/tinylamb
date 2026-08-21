/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/query_memory.hpp"

#include <cstdlib>
#include <string>

#include "type/value.hpp"

namespace tinylamb {
namespace {
constexpr size_t kDefaultQueryMemoryBytes = size_t{1} << 30;  // 1 GiB
}

size_t QueryMemoryBudget::LimitFromEnv() {
  const char* env = std::getenv("TINYLAMB_QUERY_MEMORY_BYTES");
  if (env == nullptr || env[0] == '\0') {
    return kDefaultQueryMemoryBytes;
  }
  const unsigned long long bytes = std::strtoull(env, nullptr, 10);
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
