/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_SORT_EXECUTOR_HPP
#define TINYLAMB_SORT_EXECUTOR_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "executor/pipeline_breaker.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class SortExecutor : public ExecutorBase, public PipelineBreaker {
 public:
  // Rows a merge window holds while spilled runs are merged; matches the
  // optimizer's SortMemoryPolicy default.
  static constexpr size_t kDefaultMergeWindowRows = 128;

  // Where the rows live while the sort orders them. Every mode keeps this
  // executor's own spill selection as the last resort, so a shape chosen from a
  // bad estimate still returns the right answer — the mode only decides which
  // shape starts out and which bound it respects.
  enum class SpillMode : uint8_t { kAuto, kBuffered, kExternalMerge };

  // Physical knobs the plan hands down (BufferedSortPlan /
  // ExternalMergeSortPlan); the plan layer keeps the logical version of the
  // same contract.
  struct Options {
    SpillMode spill_mode{SpillMode::kAuto};
    // 0 = the process-wide query memory budget decides, the historical
    // behavior.
    size_t memory_budget_bytes{0};
    size_t merge_window_rows{kDefaultMergeWindowRows};
    // External merge only: rows buffered before a sorted run is written.
    // 0 = bound the run by the memory budget alone.
    size_t spill_threshold_rows{0};
  };

  struct Key {
    Expression expression;
    bool ascending{true};
    std::optional<bool> nulls_first = std::nullopt;
    // Collation the key's comparison runs under, resolved from the key
    // expression by the planner (COLLATE(...)). kNone keeps the engine
    // default; a case-insensitive key folds its text before the memcomparable
    // encoding, so the in-memory sort and the external merge runs agree.
    Collation collation{Collation::kNone};
  };
  SortExecutor(Executor source, Schema schema, std::vector<Key> keys,
               size_t worker_count = 1)
      : SortExecutor(std::move(source), std::move(schema), std::move(keys),
                     worker_count, Options{}) {}
  SortExecutor(Executor source, Schema schema, std::vector<Key> keys,
               size_t worker_count, Options options)
      : source_(std::move(source)),
        schema_(std::move(schema)),
        keys_(std::move(keys)),
        options_(options),
        worker_count_(std::max<size_t>(1, worker_count)) {}
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& output, int indent) const override;

  [[nodiscard]] const Options& SortOptions() const { return options_; }
  [[nodiscard]] SpillMode ActiveSpillMode() const {
    return options_.spill_mode;
  }
  [[nodiscard]] size_t MergeWindowRows() const {
    return std::max<size_t>(1, options_.merge_window_rows);
  }
  [[nodiscard]] static std::string_view SpillModeLabel(SpillMode mode) {
    switch (mode) {
      case SpillMode::kBuffered:
        return "buffered";
      case SpillMode::kExternalMerge:
        return "external-merge";
      case SpillMode::kAuto:
        break;
    }
    return "adaptive";
  }

  // PipelineBreaker interface
  [[nodiscard]] bool IsMaterialized() const override { return materialized_; }
  void MaterializePipeline() override { FailWith(Materialize()); }
  [[nodiscard]] size_t MaterializedRowCount() const override {
    return rows_.size();
  }
  [[nodiscard]] size_t MaterializedBytes() const override {
    return rows_charge_.Bytes();
  }

 private:
  Status Materialize();
  Executor source_;
  Schema schema_;
  std::vector<Key> keys_;
  Options options_{};
  std::vector<std::pair<Row, RowPosition>> rows_;
  size_t offset_{0};
  bool materialized_{false};
  size_t worker_count_;
  QueryMemoryCharge rows_charge_;
};
}  // namespace tinylamb
#endif
