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
  PipelineBreaker() = default;
  // Non-copyable by design: instances are owned by smart pointers and
  // referenced by raw pointers throughout the executor/graph object web.
  PipelineBreaker(const PipelineBreaker&) = delete;
  PipelineBreaker& operator=(const PipelineBreaker&) = delete;
  PipelineBreaker(PipelineBreaker&&) = delete;
  PipelineBreaker& operator=(PipelineBreaker&&) = delete;
  virtual ~PipelineBreaker() = default;

  // Deriving from PipelineBreaker is what marks an operator as a pipeline
  // boundary; the pure-virtual materialize methods below are the contract a
  // boundary has to honour. IsPipelineBreaker() therefore defaults to true.
  // It stays virtual for interface consistency with the other queries and so
  // an out-of-tree operator can override it if it needs a different answer;
  // no in-tree caller currently depends on the trait itself.
  [[nodiscard]] virtual bool IsPipelineBreaker() const { return true; }
  [[nodiscard]] virtual bool IsMaterialized() const = 0;
  virtual void MaterializePipeline() = 0;
  [[nodiscard]] virtual size_t MaterializedRowCount() const = 0;
  [[nodiscard]] virtual size_t MaterializedBytes() const = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PIPELINE_BREAKER_HPP
