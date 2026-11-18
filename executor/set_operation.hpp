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
#include "executor/distinct.hpp"
#include "executor/executor_base.hpp"
#include "executor/spill_file.hpp"
#include "type/row.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

// Set operations use SQL equality rather than Value::operator==.  In
// particular, all NaN values belong to one DISTINCT/GROUP key, while the
// ordinary Value equality intentionally keeps IEEE NaN semantics.
struct SetOperationRowEqual {
  bool operator()(const Row& left, const Row& right) const;
};

// SQL equality folds case-insensitive VARCHAR text, so the hash must fold the
// same text or CI peers land in different buckets and are never compared.
using SetOperationRowSet =
    std::unordered_set<Row, DistinctRowHash, SetOperationRowEqual>;
using SetOperationRowMap =
    std::unordered_map<Row, size_t, DistinctRowHash, SetOperationRowEqual>;

class SetOperationExecutor : public ExecutorBase {
 public:
  SetOperationExecutor(std::vector<Executor> sources,
                       SetOperationKind operation)
      : sources_(std::move(sources)), operation_(operation) {}

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  struct Positioned {
    Row row;
    RowPosition position;
  };

  Status Materialize();
  void AppendAll(const std::vector<Positioned>& source);
  void AppendDistinct(const std::vector<Positioned>& source,
                      SetOperationRowSet* seen);
  void AppendIntersection(const std::vector<std::vector<Positioned>>& rows,
                          bool all);
  void AppendExcept(const std::vector<std::vector<Positioned>>& rows, bool all);
  Status MaterializePartitioned();
  Status MaterializeRows(std::vector<std::vector<Positioned>> rows);

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
