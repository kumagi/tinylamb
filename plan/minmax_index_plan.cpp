/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/minmax_index_plan.hpp"

#include <ostream>
#include <utility>

#include "common/constants.hpp"
#include "expression/column_value.hpp"
#include "plan/plan.hpp"
#include "type/column.hpp"
#include "type/type.hpp"

namespace tinylamb {

MinMaxIndexPlan::MinMaxIndexPlan(Plan child, NamedExpression aggregate,
                                 size_t value_slot, bool reverse)
    : child_(std::move(child)),
      aggregate_(std::move(aggregate)),
      value_slot_(value_slot),
      reverse_(reverse),
      schema_([&] {
        ValueType value_type = ValueType::kInt64;
        if (aggregate_.expression &&
            aggregate_.expression->Type() == TypeTag::kAggregateExp) {
          const auto& expression = aggregate_.expression->AsAggregateExpression();
          try {
            const Type result = expression.Child()->ResultType(child_->GetSchema());
            switch (result.GetType()) {
              case TypeTag::kDouble:
                value_type = ValueType::kDouble;
                break;
              case TypeTag::kVarChar:
                value_type = ValueType::kVarChar;
                break;
              case TypeTag::kDate:
                value_type = ValueType::kDate;
                break;
              default:
                break;
            }
          } catch (...) {
          }
        }
        return Schema("", {Column(aggregate_.name, value_type)});
      }()) {}

void MinMaxIndexPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << ToString() << "\n";
  child_->Dump(output, indent + 2);
}

std::string MinMaxIndexPlan::ToString() const {
  const auto& aggregate = aggregate_.expression->AsAggregateExpression();
  const bool is_max = aggregate.GetType() == AggregationType::kMax;
  std::string result = "MinMaxIndexScan ";
  result += is_max ? "MAX" : "MIN";
  result += " limit=1";
  if (reverse_) {
    result += " reverse";
  }
  return result;
}

}  // namespace tinylamb
