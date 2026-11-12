/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_PARALLEL_WORK_HPP
#define TINYLAMB_COMMON_PARALLEL_WORK_HPP

#include <cstddef>
#include <functional>

#include "common/status_or.hpp"

namespace tinylamb {
// Bound a team by available work, the query's admission budget and the
// caller's allowed CPU set. No topology/model-specific core-count cutoff.
[[nodiscard]] size_t ParallelWorkerCount(size_t tasks);

// Execute each index in [0, count) once and wait for every callback. Index 0
// uses the caller; helpers are reused across calls. Callbacks may run in any
// order, so deterministic operators keep their own indexed shards. Nested
// teams run serially to avoid waiting for helpers occupied by their parents.
// Every helper inherits the coordinator's CPU affinity and admission budget.
// Exceptions are translated to Status only after all callbacks have finished.
Status RunParallelTasks(size_t count, const std::function<void(size_t)>& task);
}  // namespace tinylamb
#endif
