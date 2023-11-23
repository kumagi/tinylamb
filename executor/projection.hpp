/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_PROJECTION_HPP
#define TINYLAMB_PROJECTION_HPP

#include <memory>
#include <utility>
#include <vector>

#include "executor_base.hpp"
#include "expression/named_expression.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Projection : public ExecutorBase {
 public:
  Projection(std::vector<NamedExpression> expressions, Schema input_schema,
             Executor src)
      : expressions_(std::move(expressions)),
        input_schema_(std::move(input_schema)),
        src_(std::move(src)) {}
  Projection(const Projection&) = delete;
  Projection(Projection&&) = delete;
  Projection& operator=(const Projection&) = delete;
  Projection& operator=(Projection&&) = delete;
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
