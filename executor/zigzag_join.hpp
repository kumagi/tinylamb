#ifndef TINYLAMB_ZIGZAG_JOIN_HPP
#define TINYLAMB_ZIGZAG_JOIN_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class ZigZagJoin final : public ExecutorBase {
 public:
  ZigZagJoin(Executor left, slot_t left_key, Executor right, slot_t right_key,
             BinaryOperation operation, Expression predicate,
             Schema output_schema);
  ZigZagJoin(Executor left, const std::vector<slot_t>& left_keys,
             Executor right, const std::vector<slot_t>& right_keys,
             BinaryOperation operation, Expression predicate,
             Schema output_schema);
  ZigZagJoin(const ZigZagJoin&) = delete;
  ZigZagJoin(ZigZagJoin&&) = delete;
  ZigZagJoin& operator=(const ZigZagJoin&) = delete;
  ZigZagJoin& operator=(ZigZagJoin&&) = delete;
  ~ZigZagJoin() override = default;

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& output, int indent) const override;

  [[nodiscard]] BinaryOperation Operation() const { return operation_; }
  [[nodiscard]] BinaryOperation Comparison() const { return operation_; }

 private:
  struct PositionedRow {
    Row row;
    RowPosition position;
  };

  struct OrderState {
    std::optional<int64_t> previous;
  };

  bool NextRow(Row* destination, RowPosition* position);
  bool LoadLeft();
  bool LoadRight();
  void ClearLeft();
  [[nodiscard]] StatusOr<bool> PairPasses(const Row& left,
                                          const Row& right) const;
  [[nodiscard]] std::pair<size_t, size_t> CandidateRange(int64_t left) const;

  Executor left_;
  Executor right_;
  slot_t left_key_{};
  slot_t right_key_{};
  BinaryOperation operation_{BinaryOperation::kEquals};
  Expression predicate_;
  std::vector<Expression> predicate_conjuncts_;
  Schema output_schema_;

  bool have_left_{false};
  PositionedRow current_left_;
  std::optional<int64_t> left_value_;
  OrderState left_order_;

  bool right_loaded_{false};
  std::vector<PositionedRow> right_rows_;
  OrderState right_order_;
  size_t right_end_{0};
  size_t right_cursor_{0};
};

}  // namespace tinylamb

#endif
