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

#ifndef TINYLAMB_SELECTION_HPP
#define TINYLAMB_SELECTION_HPP

#include <memory>

#include "executor_base.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class ExpressionBase;

class Selection : public ExecutorBase {
 public:
  Selection(Expression exp, Schema schema, Executor src);
  Selection(const Selection&) = delete;
  Selection(Selection&&) = delete;
  Selection& operator=(const Selection&) = delete;
  Selection& operator=(Selection&&) = delete;
  ~Selection() override = default;

  bool Next(Row* dst, RowPosition* rp) override;

  void Dump(std::ostream& o, int indent) const override;

 private:
  Expression exp_;
  Schema schema_;
  Executor src_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_SELECTION_HPP
