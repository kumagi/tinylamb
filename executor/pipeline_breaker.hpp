/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PIPELINE_BREAKER_HPP
#define TINYLAMB_EXECUTOR_PIPELINE_BREAKER_HPP

#include <cstddef>

namespace tinylamb {

// Explicit interface for physical operators that break pipeline streaming by
// materializing an entire input side (e.g. HashJoin build, Sort, Materialize)
// before producing output tuples.
class PipelineBreaker {
 public:
  virtual ~PipelineBreaker() = default;

  [[nodiscard]] virtual bool IsPipelineBreaker() const { return true; }
  [[nodiscard]] virtual bool IsMaterialized() const = 0;
  virtual void MaterializePipeline() = 0;
  [[nodiscard]] virtual size_t MaterializedRowCount() const = 0;
  [[nodiscard]] virtual size_t MaterializedBytes() const = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PIPELINE_BREAKER_HPP
