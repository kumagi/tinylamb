/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
#include <cmath>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "executor/detail/pipeline/pipeline_ops.hpp"
#include "executor/executor_base.hpp"
#include "expression/window_function_expression.hpp"

namespace tinylamb::pipeline_p3b_test {

class TestKnobsGuard {
 public:
  TestKnobsGuard() = default;
  explicit TestKnobsGuard(size_t op_budget_bytes, bool always_spill = false,
                          size_t temp_limit_bytes = 0) {
    ProcessMemoryBudget::Global().SetTestOpMemoryBytes(op_budget_bytes);
    ProcessMemoryBudget::Global().SetTestAlwaysSpill(always_spill);
    ProcessMemoryBudget::Global().SetTestTempLimitBytes(temp_limit_bytes);
  }
  TestKnobsGuard(const TestKnobsGuard&) = delete;
  TestKnobsGuard& operator=(const TestKnobsGuard&) = delete;
  TestKnobsGuard(TestKnobsGuard&&) = delete;
  TestKnobsGuard& operator=(TestKnobsGuard&&) = delete;
  ~TestKnobsGuard() { ProcessMemoryBudget::Global().ResetTestKnobs(); }
};

inline std::vector<Row> DrainExecutor(ExecutorBase* exec) {
  std::vector<Row> out;
  DataChunk chunk;
  while (exec->NextBatch(&chunk, kDefaultVectorSize) > 0) {
    for (size_t i = 0; i < chunk.Size(); ++i) {
      out.push_back(chunk.RowAt(i));
    }
    chunk.Reset();
  }
  return out;
}

inline std::vector<Row> ExecuteSink(
    Schema out_schema, std::vector<Row> input_rows,
    std::shared_ptr<PipelineSink> sink, Status* status_out,
    PipelineExecutionMetrics* metrics_out = nullptr,
    TransactionContext* txn = nullptr) {
  auto binding = std::make_shared<DeferredPipelineSource>("SinkOut");
  PipelineGraph g;
  g.output_schema = std::move(out_schema);
  g.stages.push_back(PipelineStage{
      .id = 0,
      .name = "Stage0",
      .source = std::make_shared<RowVectorSource>(std::move(input_rows)),
      .operators = {},
      .sink = std::move(sink),
      .output_binding = binding,
  });
  g.stages.push_back(PipelineStage{
      .id = 1,
      .name = "Root",
      .source = binding,
      .operators = {},
      .sink = nullptr,
      .output_binding = nullptr,
  });
  PipelineExecutor exec(txn, std::move(g));
  std::vector<Row> out = DrainExecutor(&exec);
  if (status_out != nullptr) {
    *status_out = exec.GetStatus();
  }
  if (metrics_out != nullptr) {
    *metrics_out = exec.Metrics();
  }
  return out;
}

inline bool RowsApproxEqual(const Row& a, const Row& b) {
  if (a.Count() != b.Count()) {
    return false;
  }
  for (size_t i = 0; i < a.Count(); ++i) {
    if (a[i].IsNull() && b[i].IsNull()) {
      continue;
    }
    if (a[i].IsNull() || b[i].IsNull()) {
      return false;
    }
    if (a[i].type == ValueType::kDouble && b[i].type == ValueType::kDouble) {
      if (std::abs(a[i].AsDouble() - b[i].AsDouble()) > 1e-9) {
        return false;
      }
      continue;
    }
    if (!(a[i] == b[i])) {
      return false;
    }
  }
  return true;
}

inline Expression MakeWindowCall(
    std::string fn, std::vector<Expression> args,
    std::vector<WindowOrderTerm> order_by,
    WindowFrameUnit unit = WindowFrameUnit::kDefault,
    WindowFrameBound start = {.type = WindowFrameBoundType::kUnboundedPreceding,
                              .offset = nullptr},
    WindowFrameBound end = {.type = WindowFrameBoundType::kCurrentRow,
                            .offset = nullptr},
    bool has_frame = false) {
  auto call = std::make_shared<WindowFunctionCallExpression>();
  call->function = std::move(fn);
  call->args = std::move(args);
  call->order_by = std::move(order_by);
  call->frame_unit = unit;
  call->frame_start = std::move(start);
  call->frame_end = std::move(end);
  call->has_frame = has_frame;
  return call;
}

}  // namespace tinylamb::pipeline_p3b_test
