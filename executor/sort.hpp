/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_SORT_EXECUTOR_HPP
#define TINYLAMB_SORT_EXECUTOR_HPP

#include <algorithm>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "executor/pipeline_breaker.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class SortExecutor : public ExecutorBase, public PipelineBreaker {
 public:
  struct Key {
    Expression expression;
    bool ascending{true};
    std::optional<bool> nulls_first;
  };
  SortExecutor(Executor source, Schema schema, std::vector<Key> keys,
               size_t worker_count = 1)
      : source_(std::move(source)),
        schema_(std::move(schema)),
        keys_(std::move(keys)),
        worker_count_(std::max<size_t>(1, worker_count)) {}
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& output, int indent) const override;

  // PipelineBreaker interface
  [[nodiscard]] bool IsMaterialized() const override { return materialized_; }
  void MaterializePipeline() override { Materialize(); }
  [[nodiscard]] size_t MaterializedRowCount() const override {
    return rows_.size();
  }
  [[nodiscard]] size_t MaterializedBytes() const override {
    return rows_charge_.Bytes();
  }

 private:
  void Materialize();
  Executor source_;
  Schema schema_;
  std::vector<Key> keys_;
  std::vector<std::pair<Row, RowPosition>> rows_;
  size_t offset_{0};
  bool materialized_{false};
  size_t worker_count_;
  QueryMemoryCharge rows_charge_;
};
}  // namespace tinylamb
#endif
