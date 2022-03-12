//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_SELECTION_HPP
#define TINYLAMB_SELECTION_HPP

#include <memory>

#include "executor_base.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Expression;
class ExpressionBase;

class Selection : public ExecutorBase {
 public:
  Selection(Expression& exp, Schema schema,
            std::unique_ptr<ExecutorBase>&& src);
  ~Selection() override = default;
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Expression exp_;
  Schema schema_;
  std::unique_ptr<ExecutorBase> src_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_SELECTION_HPP
