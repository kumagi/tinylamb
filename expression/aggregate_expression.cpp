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

#include "expression/aggregate_expression.hpp"

#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {

std::unordered_set<ColumnName> AggregateExpression::TouchedColumns() const {
  return child_->TouchedColumns();
}

Value AggregateExpression::Evaluate(const Row& /*row*/, const Schema& /*schema*/) const {
  // This method must not be called directly: the value of an aggregate
  // expression is calculated by the aggregator.
  throw std::logic_error("aggregate expression cannot be evaluated directly");
}

Type AggregateExpression::ResultType(const Schema& schema) const {
  if (type_ == AggregationType::kCount) {
    return {TypeTag::kBigInt};
  }
  if (type_ == AggregationType::kAvg) {
    return {TypeTag::kDouble};
  }
  return child_->ResultType(schema);
}

Type AggregateExpression::ResultType(const Schema& left,
                                     const Schema& right) const {
  if (type_ == AggregationType::kCount) {
    return {TypeTag::kBigInt};
  }
  if (type_ == AggregationType::kAvg) {
    return {TypeTag::kDouble};
  }
  return child_->ResultType(left, right);
}

std::string AggregateExpression::ToString() const {
  return ::tinylamb::ToString(type_) + "(" + (distinct_ ? "DISTINCT " : "") +
         child_->ToString() + ")";
}

void AggregateExpression::Dump(std::ostream& o) const {
  o << ::tinylamb::ToString(type_) << "(" << (distinct_ ? "DISTINCT " : "")
    << *child_ << ")";
}

}  // namespace tinylamb
