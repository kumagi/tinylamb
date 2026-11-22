/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "table/workload/workload.hpp"

#include <limits>

#include "type/schema.hpp"

namespace tinylamb {

namespace {
ColumnWorkload& GetOrCreate(std::unordered_map<ColumnName, ColumnWorkload>& map,
                            const ColumnName& col) {
  auto [it, _] = map.try_emplace(col);
  return it->second;
}

const ColumnName* ResolveSlot(const Schema& sc, slot_t col) {
  if (col >= sc.ColumnCount()) {
    return nullptr;
  }
  return &sc.GetColumn(col).Name();
}
}  // namespace

void WorkloadTracker::RecordFilter(const ColumnName& col) {
  std::scoped_lock lock(mu_);
  GetOrCreate(counts_, col).filter.fetch_add(1, std::memory_order_relaxed);
}

void WorkloadTracker::RecordAggregate(const ColumnName& col) {
  std::scoped_lock lock(mu_);
  GetOrCreate(counts_, col).aggregate.fetch_add(1, std::memory_order_relaxed);
}

void WorkloadTracker::RecordPointLookup(const ColumnName& col) {
  std::scoped_lock lock(mu_);
  GetOrCreate(counts_, col)
      .point_lookup.fetch_add(1, std::memory_order_relaxed);
}

void WorkloadTracker::RecordSortKey(const ColumnName& col) {
  std::scoped_lock lock(mu_);
  GetOrCreate(counts_, col).sort_key.fetch_add(1, std::memory_order_relaxed);
}

void WorkloadTracker::RecordFilter(const Schema& sc, slot_t col) {
  if (const ColumnName* name = ResolveSlot(sc, col)) {
    RecordFilter(*name);
  }
}

void WorkloadTracker::RecordAggregate(const Schema& sc, slot_t col) {
  if (const ColumnName* name = ResolveSlot(sc, col)) {
    RecordAggregate(*name);
  }
}

void WorkloadTracker::RecordPointLookup(const Schema& sc, slot_t col) {
  if (const ColumnName* name = ResolveSlot(sc, col)) {
    RecordPointLookup(*name);
  }
}

void WorkloadTracker::RecordSortKey(const Schema& sc, slot_t col) {
  if (const ColumnName* name = ResolveSlot(sc, col)) {
    RecordSortKey(*name);
  }
}

uint64_t WorkloadTracker::Count(const ColumnName& col) const {
  std::scoped_lock lock(mu_);
  auto it = counts_.find(col);
  return it == counts_.end() ? 0 : it->second.Total();
}

uint64_t WorkloadTracker::TotalEvents() const {
  std::scoped_lock lock(mu_);
  uint64_t total = 0;
  for (const auto& [name, w] : counts_) {
    total += w.Total();
  }
  return total;
}

AccessHint WorkloadTracker::InferLocked(const ColumnWorkload& w) {
  const uint64_t f = w.filter.load(std::memory_order_relaxed);
  const uint64_t a = w.aggregate.load(std::memory_order_relaxed);
  const uint64_t p = w.point_lookup.load(std::memory_order_relaxed);
  const uint64_t s = w.sort_key.load(std::memory_order_relaxed);
  // Saturating sum: counters only ever grow, so four near-UINT64_MAX values
  // must not wrap `total` back below kMinSamples and flip a stable inference
  // to kUnknown.
  constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
  const auto sat_add = [](uint64_t x, uint64_t y) {
    return x > kMax - y ? kMax : x + y;
  };
  const uint64_t total = sat_add(sat_add(f, a), sat_add(p, s));
  if (total < kMinSamples) {
    return AccessHint::kUnknown;
  }
  // Strict majority (>50%), written as x > total - x so the comparison
  // cannot overflow even when total has saturated. Ties stay kUnknown.
  if (f > total - f) {
    return AccessHint::kFilter;
  }
  if (a > total - a) {
    return AccessHint::kAggregate;
  }
  if (p > total - p) {
    return AccessHint::kPointLookup;
  }
  if (s > total - s) {
    return AccessHint::kSortKey;
  }
  return AccessHint::kUnknown;
}

AccessHint WorkloadTracker::InferHint(const ColumnName& col) const {
  std::scoped_lock lock(mu_);
  auto it = counts_.find(col);
  if (it == counts_.end()) {
    return AccessHint::kUnknown;
  }
  return InferLocked(it->second);
}

size_t WorkloadTracker::ApplyTo(Schema& sc) {
  std::scoped_lock lock(mu_);
  size_t changed = 0;
  for (slot_t c = 0; c < sc.ColumnCount(); ++c) {
    const ColumnName& name = sc.GetColumn(c).Name();
    auto it = counts_.find(name);
    if (it == counts_.end()) {
      continue;
    }
    const AccessHint inferred = InferLocked(it->second);
    if (inferred == AccessHint::kUnknown) {
      continue;
    }
    const AccessHint current = sc.GetColumn(c).Hint();
    auto ap = applied_.find(name);
    // Lineage is by value: a manual SetHint that happens to equal our last
    // applied hint is indistinguishable from our own write and will follow
    // later workload shifts (documented on WorkloadTracker::ApplyTo).
    const bool eligible = current == AccessHint::kUnknown ||
                          (ap != applied_.end() && ap->second == current);
    if (eligible && inferred != current) {
      sc.SetColumnHint(c, inferred);
      applied_[name] = inferred;
      ++changed;
    }
  }
  return changed;
}

void WorkloadTracker::Reset() {
  std::scoped_lock lock(mu_);
  // Only the observation window is cleared: applied_ lineage deliberately
  // survives so previously-inferred columns keep following workload shifts.
  counts_.clear();
}

}  // namespace tinylamb
