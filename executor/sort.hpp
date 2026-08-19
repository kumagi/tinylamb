/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_SORT_EXECUTOR_HPP
#define TINYLAMB_SORT_EXECUTOR_HPP

#include <algorithm>
#include <utility>
#include <thread>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class SortExecutor : public ExecutorBase {
 public:
  struct Key {
    Expression expression;
    bool ascending{true};
  };
  SortExecutor(Executor source, Schema schema, std::vector<Key> keys,
               size_t worker_count = std::thread::hardware_concurrency())
      : source_(std::move(source)),
        schema_(std::move(schema)),
        keys_(std::move(keys)),
        worker_count_(std::max<size_t>(1, worker_count)) {}
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  void Materialize();
  Executor source_;
  Schema schema_;
  std::vector<Key> keys_;
  std::vector<std::pair<Row, RowPosition>> rows_;
  size_t offset_{0};
  bool materialized_{false};
  size_t worker_count_;
};
}  // namespace tinylamb
#endif
