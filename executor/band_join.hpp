/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_BAND_JOIN_HPP
#define TINYLAMB_BAND_JOIN_HPP

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class BandJoin final : public ExecutorBase {
 public:
  BandJoin(Executor left, std::vector<slot_t> left_columns, Executor right,
           std::vector<slot_t> right_columns, BinaryOperation comparison,
           Expression predicate, Schema residual_schema,
           JoinKind kind = JoinKind::kInner, size_t left_width = 0,
           size_t right_width = 0, Schema output_schema = Schema());

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] JoinKind Kind() const { return kind_; }
  [[nodiscard]] BinaryOperation Comparison() const { return comparison_; }

 private:
  using PositionedRow = std::pair<Row, RowPosition>;

  struct RightKey {
    int64_t value{0};
    size_t row_index{0};
  };

  void EnsureMaterialized();
  Status Materialize();
  [[nodiscard]] static StatusOr<std::optional<int64_t>> ReadKey(const Row& row,
                                                                slot_t column);
  [[nodiscard]] StatusOr<bool> PairPasses(const Row& left,
                                          const Row& right) const;
  [[nodiscard]] std::pair<size_t, size_t> CandidateRange(int64_t value) const;

  Executor left_;
  std::vector<slot_t> left_columns_;
  Executor right_;
  std::vector<slot_t> right_columns_;
  BinaryOperation comparison_{BinaryOperation::kLessThan};
  Expression predicate_;
  std::vector<Expression> predicate_conjuncts_;
  Schema residual_schema_;
  Schema output_schema_;
  JoinKind kind_{JoinKind::kInner};
  size_t left_width_{0};
  size_t right_width_{0};

  std::vector<PositionedRow> left_rows_;
  std::vector<PositionedRow> right_rows_;
  std::vector<RightKey> right_keys_;
  std::vector<PositionedRow> output_;
  size_t output_offset_{0};
  bool materialized_{false};
  Status materialize_error_{Status::kSuccess};
};

}  // namespace tinylamb

#endif  // TINYLAMB_BAND_JOIN_HPP
