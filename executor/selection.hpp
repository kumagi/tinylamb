//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_SELECTION_HPP
#define TINYLAMB_SELECTION_HPP

#include <memory>

#include "executor_base.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class ExpressionBase;

class Selection : public ExecutorBase {
 public:
  Selection(std::shared_ptr<ExpressionBase> exp, Schema schema,
            std::unique_ptr<ExecutorBase>&& src);
  bool Next(Row* dst) override;
  ~Selection() override = default;

 private:
  std::shared_ptr<ExpressionBase> exp_;
  Schema schema_;
  std::unique_ptr<ExecutorBase> src_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_SELECTION_HPP
