//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_SELECTION_HPP
#define TINYLAMB_SELECTION_HPP

#include <memory>

#include "executor_base.hpp"
#include "type/row.hpp"

namespace tinylamb {
class ExpressionBase;
class Schema;

class Selection : public ExecutorBase {
 public:
  Selection(std::unique_ptr<ExpressionBase>&& exp, Schema* schema,
            ExecutorBase* src);
  bool Next(Row* dst) override;
  ~Selection() override = default;

 private:
  std::unique_ptr<ExpressionBase> exp_;
  Schema* schema_;
  ExecutorBase* src_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_SELECTION_HPP
