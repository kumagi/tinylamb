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

#ifndef TINYLAMB_COMMON_PLATFORM_CPU_AFFINITY_HPP
#define TINYLAMB_COMMON_PLATFORM_CPU_AFFINITY_HPP

#include <optional>

#if defined(__linux__)
#include <sched.h>
#endif

namespace tinylamb::platform {

// A set of CPUs a thread may run on. Thread pinning exists only on Linux
// (sched_{get,set}affinity); macOS offers affinity *hints* at best, so there
// CurrentThread() returns nullopt and callers skip pinning entirely.
class CpuSet {
 public:
  // Whether this platform can pin threads to CPUs at all.
  static constexpr bool Supported() {
#if defined(__linux__)
    return true;
#else
    return false;
#endif
  }

  // The calling thread's allowed CPUs, or nullopt when unsupported/failed.
  static std::optional<CpuSet> CurrentThread();

  // A set containing only `cpu`.
  static CpuSet Single(int cpu);

  // Restricts the calling thread to this set. Returns false on failure (and
  // always where unsupported).
  bool ApplyToCurrentThread() const;

  int Count() const;
  // The lowest CPU id in the set, or -1 when empty.
  int First() const;

  bool operator==(const CpuSet& other) const;

 private:
#if defined(__linux__)
  cpu_set_t set_{};
#endif
};

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_CPU_AFFINITY_HPP
