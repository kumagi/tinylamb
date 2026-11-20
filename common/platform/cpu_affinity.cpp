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

#include "common/platform/cpu_affinity.hpp"

#include <cstring>

namespace tinylamb::platform {

#ifdef __linux__

std::optional<CpuSet> CpuSet::CurrentThread() {
  CpuSet result;
  if (::sched_getaffinity(0, sizeof(result.set_), &result.set_) != 0) {
    return std::nullopt;
  }
  return result;
}

CpuSet CpuSet::Single(int cpu) {
  CpuSet result;
  CPU_ZERO(&result.set_);
  if (cpu >= 0 && cpu < CPU_SETSIZE) {
    CPU_SET(cpu, &result.set_);
  }
  return result;
}

bool CpuSet::ApplyToCurrentThread() const {
  return ::sched_setaffinity(0, sizeof(set_), &set_) == 0;
}

int CpuSet::Count() const { return CPU_COUNT(&set_); }

int CpuSet::First() const {
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &set_)) {
      return cpu;
    }
  }
  return -1;
}

bool CpuSet::operator==(const CpuSet& other) const {
  // Cost contract (P45): ParallelPool::Work() compares this once per task
  // batch, so equality must stay a single fixed-size memcmp of the POD
  // bitmask. glibc's CPU_EQUAL expands to exactly this, but on other libcs
  // it can fall back to a word-looping helper, so compare the raw words.
  return std::memcmp(&set_, &other.set_, sizeof(set_)) == 0;
}

#else

std::optional<CpuSet> CpuSet::CurrentThread() { return std::nullopt; }
CpuSet CpuSet::Single(int /*cpu*/) { return {}; }
bool CpuSet::ApplyToCurrentThread() const { return false; }
int CpuSet::Count() const { return 0; }
int CpuSet::First() const { return -1; }
bool CpuSet::operator==(const CpuSet& /*other*/) const { return true; }

#endif

}  // namespace tinylamb::platform
