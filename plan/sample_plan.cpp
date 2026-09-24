#include "plan/sample_plan.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

std::optional<double> ConstantNumber(const Expression& expression) {
  if (!expression || expression->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  const Value& value = expression->AsConstantValue().GetValue();
  if (value.IsNull()) {
    return std::nullopt;
  }
  if (value.type == ValueType::kInt64) {
    return static_cast<double>(value.value.int_value);
  }
  if (value.type == ValueType::kDouble) {
    return value.value.double_value;
  }
  return std::nullopt;
}

std::optional<uint64_t> ConstantRows(const Expression& expression) {
  if (!expression || expression->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  const Value& value = expression->AsConstantValue().GetValue();
  if (value.type != ValueType::kInt64 || value.IsNull()) {
    return std::nullopt;
  }
  if (!value.IsUnsigned() && value.value.int_value < 0) {
    return std::nullopt;
  }
  if (value.IsUnsigned()) {
    return static_cast<uint64_t>(value.value.int_value);
  }
  return static_cast<uint64_t>(value.value.int_value);
}

}  // namespace

SamplePlan::SamplePlan(Plan child, TableSampleSpec spec)
    : child_(std::move(child)),
      spec_(std::move(spec)),
      output_schema_(BuildSchema(child_->GetSchema(), spec_)) {}

size_t SamplePlan::EmitRowCount() const {
  const double input = static_cast<double>(child_->EmitRowCount());
  if (spec_.method == TableSampleMethod::kReservoir) {
    if (!spec_.partition_by.empty()) {
      return child_->EmitRowCount();
    }
    const std::optional<uint64_t> rows = ConstantRows(spec_.size);
    if (!rows.has_value()) {
      return child_->EmitRowCount();
    }
    const uint64_t limit = std::min<uint64_t>(
        *rows, static_cast<uint64_t>(std::numeric_limits<size_t>::max()));
    return static_cast<size_t>(
        std::min<double>(input, static_cast<double>(limit)));
  }
  const std::optional<double> percent = ConstantNumber(spec_.size);
  if (!percent.has_value() || !std::isfinite(*percent) || *percent < 0.0 ||
      *percent > 100.0) {
    return child_->EmitRowCount();
  }
  return static_cast<size_t>(std::ceil(input * (*percent / 100.0)));
}

bool SamplePlan::ContainsParameterSlots() const {
  return ContainsParameterSlot(spec_.size) ||
         (spec_.repeatable_seed &&
          ContainsParameterSlot(*spec_.repeatable_seed)) ||
         std::ranges::any_of(spec_.partition_by, ContainsParameterSlot) ||
         child_->ContainsParameterSlots();
}

bool SamplePlan::InstallParameterSlots(const std::vector<Value>& parameters,
                                       size_t* cursor,
                                       std::vector<Expression>* slots) {
  return SlotizeInPlace(&spec_.size, parameters, cursor, slots) &&
         ((!spec_.repeatable_seed ||
           SlotizeInPlace(&*spec_.repeatable_seed, parameters, cursor,
                          slots)) &&
          std::ranges::all_of(spec_.partition_by,
                              [&](Expression& partition) {
                                return SlotizeInPlace(&partition, parameters,
                                                      cursor, slots);
                              }) &&
          child_->InstallParameterSlots(parameters, cursor, slots));
}

void SamplePlan::Dump(std::ostream& o, int indent) const {
  o << "Sample: ";
  spec_.Dump(o);
  o << " (estimated rows: " << EmitRowCount() << ")\n"
    << Indent(static_cast<size_t>(indent) + 2);
  child_->Dump(o, indent + 2);
}

std::string SamplePlan::ToString() const {
  std::ostringstream out;
  spec_.Dump(out);
  return "Sample: " + out.str() +
         " (estimated rows: " + std::to_string(EmitRowCount()) + ")";
}

Schema SamplePlan::BuildSchema(const Schema& child,
                               const TableSampleSpec& spec) {
  std::vector<Column> columns;
  columns.reserve(static_cast<size_t>(child.ColumnCount()) +
                  (spec.with_weight ? 1U : 0U));
  for (size_t i = 0; i < child.ColumnCount(); ++i) {
    columns.push_back(child.GetColumn(i));
  }
  if (spec.with_weight) {
    std::string qualifier;
    if (child.ColumnCount() != 0) {
      qualifier = child.GetColumn(0).Name().schema;
    }
    columns.emplace_back(ColumnName(qualifier, spec.weight_alias),
                         ValueType::kDouble);
  }
  return Schema("", std::move(columns));
}

}  // namespace tinylamb
