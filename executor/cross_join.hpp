//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_CROSS_JOIN_HPP
#define TINYLAMB_CROSS_JOIN_HPP

#include <memory>
#include <unordered_map>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
struct Row;

class CrossJoin : public ExecutorBase {
 public:
  CrossJoin(Executor left, Executor right);
  CrossJoin(const CrossJoin&) = delete;
  CrossJoin(CrossJoin&&) = delete;
  CrossJoin& operator=(const CrossJoin&) = delete;
  CrossJoin& operator=(CrossJoin&&) = delete;
  ~CrossJoin() override = default;

  bool Next(Row* dst, RowPosition* rp) override;

  void Dump(std::ostream& o, int indent) const override;

 private:
  void TableConstruct();

  Executor left_;
  Executor right_;

  Row hold_left_;
  bool table_constructed_{false};
  std::vector<Row> right_table_;
  std::vector<Row>::iterator right_iter_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CROSS_JOIN_HPP
