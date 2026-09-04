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

#include "plan/selection_plan.hpp"

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp).

const Schema& SelectionPlan::GetSchema() const { return src_->GetSchema(); }

size_t SelectionPlan::AccessRowCount() const { return src_->EmitRowCount(); }

size_t SelectionPlan::EmitRowCount() const { return stats_.Rows(); }

void SelectionPlan::Dump(std::ostream& o, int indent) const {
  o << "Select: [";
  exp_->Dump(o);
  o << "] (estimated cost: " << AccessRowCount() << ")\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

std::string SelectionPlan::ToString() const {
  return "Select: [" + exp_->ToString() +
         "] (estimated cost: " + std::to_string(AccessRowCount()) + ")";
}

}  // namespace tinylamb
