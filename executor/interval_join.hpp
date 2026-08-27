/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_INTERVAL_JOIN_HPP
#define TINYLAMB_INTERVAL_JOIN_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

class IntervalJoin : public ExecutorBase {
 public:
  IntervalJoin(Executor left, Schema left_schema, Executor right,
               Schema right_schema,
               std::vector<std::pair<slot_t, slot_t>> equi_keys,
               slot_t left_interval_col, slot_t right_interval_col,
               Value lower_offset, Value upper_offset,
               bool is_left_outer = false);
  IntervalJoin(const IntervalJoin&) = delete;
  IntervalJoin(IntervalJoin&&) = delete;
  IntervalJoin& operator=(const IntervalJoin&) = delete;
  IntervalJoin& operator=(IntervalJoin&&) = delete;
  ~IntervalJoin() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] const Schema& OutputSchema() const { return output_schema_; }

 private:
  void Materialize();

  Executor left_;
  Schema left_schema_;
  Executor right_;
  Schema right_schema_;
  std::vector<std::pair<slot_t, slot_t>> equi_keys_;
  slot_t left_interval_col_;
  slot_t right_interval_col_;
  Value lower_offset_;
  Value upper_offset_;
  bool is_left_outer_;
  Schema output_schema_;

  bool materialized_{false};
  std::vector<Row> output_rows_;
  size_t cursor_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INTERVAL_JOIN_HPP
