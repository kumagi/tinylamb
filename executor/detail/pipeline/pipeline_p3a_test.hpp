/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "executor/executor_base.hpp"

namespace tinylamb::pipeline_p3a_test {

struct OpBudgetOverrideGuard {
  explicit OpBudgetOverrideGuard(size_t bytes) {
    ProcessMemoryBudget::Global().SetTestOpMemoryBytes(bytes);
  }
  OpBudgetOverrideGuard(const OpBudgetOverrideGuard&) = delete;
  OpBudgetOverrideGuard& operator=(const OpBudgetOverrideGuard&) = delete;
  OpBudgetOverrideGuard(OpBudgetOverrideGuard&&) = delete;
  OpBudgetOverrideGuard& operator=(OpBudgetOverrideGuard&&) = delete;
  ~OpBudgetOverrideGuard() {
    ProcessMemoryBudget::Global().SetTestOpMemoryBytes(0);
    ProcessMemoryBudget::Global().SetTestAlwaysSpill(false);
    ProcessMemoryBudget::Global().SetTestTempLimitBytes(0);
    ProcessMemoryBudget::Global().SetTestSpillIoFailCountdown(0);
  }
};

inline std::vector<Row> DrainExecutor(ExecutorBase* exec) {
  std::vector<Row> rows;
  Row r;
  while (exec->Next(&r, nullptr)) {
    rows.push_back(std::move(r));
  }
  return rows;
}

}  // namespace tinylamb::pipeline_p3a_test
