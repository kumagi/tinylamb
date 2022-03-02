//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_CROSS_JOIN_HPP
#define TINYLAMB_CROSS_JOIN_HPP

#include <memory>
#include <unordered_map>

#include "executor/executor_base.hpp"
#include "expression/expression_base.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Row;
class ExecutorBase;

class CrossJoin : public ExecutorBase {
 public:
  CrossJoin(std::unique_ptr<ExecutorBase>&& left,
            std::unique_ptr<ExecutorBase>&& right);

  bool Next(Row* dst) override;
  ~CrossJoin() override = default;

  void Dump(std::ostream& o, int indent) const override;

 private:
  void TableConstruct();

 private:
  std::unique_ptr<ExecutorBase> left_;
  std::unique_ptr<ExecutorBase> right_;

  Row hold_left_;
  bool table_constructed_{false};
  std::vector<Row> right_table_;
  std::vector<Row>::iterator right_iter_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_CROSS_JOIN_HPP
