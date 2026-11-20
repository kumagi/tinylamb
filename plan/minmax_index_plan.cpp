/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/minmax_index_plan.hpp"

#include <cstddef>
#include <ostream>
#include <string>
#include <utility>

#include "common/constants.hpp"
#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

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
          const auto& expression =
              aggregate_.expression->AsAggregateExpression();
          try {
            const Type result =
                expression.Child()
                    ? expression.Child()->ResultType(child_->GetSchema())
                    : Type(TypeTag::kBigInt);
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
          } catch (const std::exception& e) {
            LOG(LogLevel::kWarn) << "MinMaxIndexPlan: cannot derive aggregate result "
                            "type, defaulting to INT64: " << e.what();
          } catch (...) {
            LOG(LogLevel::kWarn) << "MinMaxIndexPlan: cannot derive aggregate result "
                            "type, defaulting to INT64";
          }
        }
        return Schema("", {Column(aggregate_.name, value_type)});
      }()) {}

void MinMaxIndexPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << ToString() << "\n";
  DumpChild(output, *child_, indent + 2);
}

std::string MinMaxIndexPlan::ToString() const {
  if (!aggregate_.expression ||
      aggregate_.expression->Type() != TypeTag::kAggregateExp) {
    return "MinMaxIndexScan invalid aggregate";
  }
  const auto& aggregate = aggregate_.expression->AsAggregateExpression();
  const auto type = aggregate.GetType();
  if (type != AggregationType::kMin && type != AggregationType::kMax) {
    return "MinMaxIndexScan invalid aggregate";
  }
  std::string result = "MinMaxIndexScan ";
  result += type == AggregationType::kMax ? "MAX" : "MIN";
  result += " limit=1";
  if (reverse_) {
    result += " reverse";
  }
  return result;
}

}  // namespace tinylamb
