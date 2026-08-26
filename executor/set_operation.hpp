/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_SET_OPERATION_EXECUTOR_HPP
#define TINYLAMB_SET_OPERATION_EXECUTOR_HPP

#include <cstddef>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/set_operation.hpp"
#include "executor/executor_base.hpp"
#include "executor/spill_file.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class SetOperationExecutor : public ExecutorBase {
 public:
  SetOperationExecutor(std::vector<Executor> sources,
                       SetOperationKind operation)
      : sources_(std::move(sources)), operation_(operation) {}

  bool Next(Row* destination, RowPosition* position) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  struct Positioned {
    Row row;
    RowPosition position;
  };

  void Materialize();
  void AppendAll(const std::vector<Positioned>& source);
  void AppendDistinct(const std::vector<Positioned>& source,
                      std::unordered_set<Row>* seen);
  void AppendIntersection(const std::vector<std::vector<Positioned>>& rows,
                          bool all);
  void AppendExcept(const std::vector<std::vector<Positioned>>& rows,
                    bool all);
  void MaterializePartitioned();
  void MaterializeRows(std::vector<std::vector<Positioned>> rows);

  std::vector<Executor> sources_;
  SetOperationKind operation_{SetOperationKind::kUnionAll};
  std::vector<Positioned> output_;
  size_t output_offset_{0};
  bool materialized_{false};
  std::vector<std::vector<SpillFile>> spill_sources_;
  std::vector<ValueType> spill_common_types_;
  std::optional<size_t> spill_width_;
};

using UnionAllExecutor = SetOperationExecutor;

}  // namespace tinylamb

#endif  // TINYLAMB_SET_OPERATION_EXECUTOR_HPP
