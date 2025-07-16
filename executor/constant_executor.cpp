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

#include "executor/constant_executor.hpp"

#include <ostream>

#include "type/schema.hpp"

namespace tinylamb {

bool ConstantExecutor::Next(Row* row, RowPosition* rp) {
  if (done_) {
    return false;
  }
  *row = row_;
  done_ = true;
  return true;
}

void ConstantExecutor::Dump(std::ostream& o, int indent) const {
  o << "ConstantExecutor";
}

}  // namespace tinylamb
