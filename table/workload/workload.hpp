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

#ifndef TINYLAMB_WORKLOAD_HPP
#define TINYLAMB_WORKLOAD_HPP

#include <atomic>
#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "common/constants.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"

namespace tinylamb {
class Schema;

// Observed access pattern of one column: raw event counters. Relaxed atomic
// increments keep concurrent scan threads safe; the map itself is
// mutex-guarded.
struct ColumnWorkload {
  std::atomic<uint64_t> filter{0};
  std::atomic<uint64_t> aggregate{0};
  std::atomic<uint64_t> point_lookup{0};
  std::atomic<uint64_t> sort_key{0};
  [[nodiscard]] uint64_t Total() const {
    return filter.load(std::memory_order_relaxed) +
           aggregate.load(std::memory_order_relaxed) +
           point_lookup.load(std::memory_order_relaxed) +
           sort_key.load(std::memory_order_relaxed);
  }
};

// Redshift-ATO-style observation window: records per-column access events
// from the scan kernels and infers the AccessHint the chunk encoder should
// use for the NEXT compaction (MergeOptions). Thread-safe. Keyed by
// ColumnName so observations survive ALTERs that only renumber slots.
//
// Lifecycle: construct one tracker per table (or per engine), pass it to
// scan calls, then periodically ApplyTo(schema) (or merge with it) and
// Reset() to start a fresh window. Counters never decay within a window;
// Reset() is the window boundary.
class WorkloadTracker {
 public:
  // Minimum events before an inference is trusted (flapping guard).
  static constexpr uint64_t kMinSamples = 16;

  WorkloadTracker() = default;
  WorkloadTracker(const WorkloadTracker&) = delete;
  WorkloadTracker& operator=(const WorkloadTracker&) = delete;

  void RecordFilter(const ColumnName& col);
  void RecordAggregate(const ColumnName& col);
  void RecordPointLookup(const ColumnName& col);
  void RecordSortKey(const ColumnName& col);
  // Convenience: resolve the slot through a schema (out-of-range slots and
  // schemas not containing the column are ignored).
  void RecordFilter(const Schema& sc, slot_t col);
  void RecordAggregate(const Schema& sc, slot_t col);
  void RecordPointLookup(const Schema& sc, slot_t col);
  void RecordSortKey(const Schema& sc, slot_t col);

  [[nodiscard]] uint64_t Count(const ColumnName& col) const;
  [[nodiscard]] uint64_t TotalEvents() const;
  // Strict-majority (>50%) inference with the kMinSamples floor; thin or
  // split workloads yield kUnknown (keep the data-shape-only choice).
  [[nodiscard]] AccessHint InferHint(const ColumnName& col) const;
  // Write inferred hints into a schema. Only columns whose current hint is
  // kUnknown or was previously inferred by THIS tracker are touched, so
  // manual SetHint calls always win. Returns the number of changed columns.
  // Lineage is tracked BY VALUE: a manual hint that coincides with the
  // tracker's last-applied hint for that column is indistinguishable from
  // an inferred one and will follow future workload shifts.
  size_t ApplyTo(Schema& sc);
  // Clear event counts (new observation window). Inference lineage is
  // retained: previously-inferred columns keep following workload shifts,
  // while manual hints stay protected. Construct a fresh tracker to drop
  // lineage as well.
  void Reset();

 private:
  // Lock-free inference core (callers hold mu_).
  static AccessHint InferLocked(const ColumnWorkload& w);

  mutable std::mutex mu_;
  std::unordered_map<ColumnName, ColumnWorkload> counts_;
  std::unordered_map<ColumnName, AccessHint> applied_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_WORKLOAD_HPP
