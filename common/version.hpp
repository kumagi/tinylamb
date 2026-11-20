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

#ifndef TINYLAMB_VERSION_HPP
#define TINYLAMB_VERSION_HPP

#include <atomic>
#include <cstdint>

namespace tinylamb {

// Process-wide monotonically increasing version counter (NOT persistent:
// restarts at 1 in every process, wraps silently on overflow).
//
// MVCC read alignment: each memtable flush / per-row write can be stamped with
// a version taken from this counter, so versions are COMPARABLE ACROSS all
// partitions and tables (unlike MemTable's per-instance generation_ counter,
// which is only meaningful within one table's read path). A future executor
// could pin a global read version V to read every partition at V; that
// cross-partition snapshot path is not yet wired up (global version mode has
// no callers).
inline uint64_t NextGlobalVersion() {
  static std::atomic<uint64_t> counter{0};
  return counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

}  // namespace tinylamb

#endif  // TINYLAMB_VERSION_HPP
