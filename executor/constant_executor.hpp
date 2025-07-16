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

#ifndef TINYLAMB_CONSTANT_EXECUTOR_HPP
#define TINYLAMB_CONSTANT_EXECUTOR_HPP

#include "executor/executor_base.hpp"
#include "type/row.hpp"

namespace tinylamb {

class ConstantExecutor : public ExecutorBase {
 public:
  explicit ConstantExecutor(Row row) : row_(std::move(row)) {}
  bool Next(Row* row, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Row row_;
  bool done_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_CONSTANT_EXECUTOR_HPP
