//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_PROJECTION_HPP
#define TINYLAMB_PROJECTION_HPP

#include <memory>
#include <utility>
#include <vector>

#include "executor_base.hpp"
#include "named_expression.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Projection : public ExecutorBase {
 public:
  Projection(std::vector<NamedExpression> expressions, Schema input_schema,
             Executor src)
      : expressions_(std::move(expressions)),
        input_schema_(std::move(input_schema)),
        src_(std::move(src)) {}
  ~Projection() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  std::vector<NamedExpression> expressions_;
  Schema input_schema_;
  Executor src_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PROJECTION_HPP
