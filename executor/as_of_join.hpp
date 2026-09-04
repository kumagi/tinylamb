/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_AS_OF_JOIN_HPP
#define TINYLAMB_AS_OF_JOIN_HPP

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

enum class AsOfComparison : uint8_t {
  kLessEqual,     // right.as_of <= left.as_of (find largest right.as_of <=
                  // left.as_of)
  kGreaterEqual,  // right.as_of >= left.as_of (find smallest right.as_of >=
                  // left.as_of)
};

class AsOfJoin : public ExecutorBase {
 public:
  AsOfJoin(Executor left, Schema left_schema, Executor right,
           Schema right_schema,
           std::vector<std::pair<slot_t, slot_t>> equi_keys,
           slot_t left_as_of_col, slot_t right_as_of_col,
           AsOfComparison comp = AsOfComparison::kLessEqual,
           bool is_left_outer = true,
           std::optional<Value> max_lookback = std::nullopt);
  AsOfJoin(const AsOfJoin&) = delete;
  AsOfJoin(AsOfJoin&&) = delete;
  AsOfJoin& operator=(const AsOfJoin&) = delete;
  AsOfJoin& operator=(AsOfJoin&&) = delete;
  ~AsOfJoin() override = default;

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
  slot_t left_as_of_col_;
  slot_t right_as_of_col_;
  AsOfComparison comparison_;
  bool is_left_outer_;
  std::optional<Value> max_lookback_;
  Schema output_schema_;

  bool materialized_{false};
  std::vector<Row> output_rows_;
  size_t cursor_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_AS_OF_JOIN_HPP
