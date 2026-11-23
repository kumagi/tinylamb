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
    return value.IsUnsigned() ? static_cast<double>(static_cast<uint64_t>(
                                    value.value.int_value))
                              : static_cast<double>(value.value.int_value);
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
  return static_cast<uint64_t>(value.value.int_value);
}

TableStatistics SampleStatistics(const TableStatistics& input,
                                const Schema& output_schema, size_t rows) {
  const TableStatistics scaled = input.ScaleCardinality(rows);
  std::vector<ColumnStats> columns;
  columns.reserve(output_schema.ColumnCount());
  for (size_t i = 0; i < input.Columns(); ++i) {
    columns.push_back(scaled.Column(i));
  }
  for (size_t i = input.Columns(); i < output_schema.ColumnCount(); ++i) {
    columns.emplace_back(output_schema.GetColumn(i).Type());
  }
  TableStatistics result(Schema("", {}));
  result.Assign(rows, std::move(columns));
  return result;
}

}  // namespace

SamplePlan::SamplePlan(Plan child, TableSampleSpec spec)
    : child_(std::move(child)),
      spec_(std::move(spec)),
      output_schema_(BuildSchema(child_->GetSchema(), spec_)),
      stats_(SampleStatistics(child_->GetStats(), output_schema_,
                              EmitRowCount())) {}

size_t SamplePlan::EmitRowCount() const {
  const auto input = static_cast<double>(child_->EmitRowCount());
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
    // Compare in the integer domain: double(limit) can round a SIZE_MAX-sized
    // limit up to 2^64, which is outside the size_t range for a narrowing cast.
    return std::min(child_->EmitRowCount(), static_cast<size_t>(limit));
  }
  const std::optional<double> percent = ConstantNumber(spec_.size);
  if (!percent.has_value() || !std::isfinite(*percent) || *percent < 0.0 ||
      *percent > 100.0) {
    return child_->EmitRowCount();
  }
  const double scaled = std::ceil(input * (*percent / 100.0));
  // A saturated child estimate (SIZE_MAX) converts to the double 2^64,
  // outside the size_t range; a sample never emits more rows than its input,
  // so clamp at the child's own count before the narrowing cast.
  return !(scaled < input) ? child_->EmitRowCount()
                            : static_cast<size_t>(scaled);
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
  return CollectParameterSlots(&spec_.size, slots) &&
         ((!spec_.repeatable_seed ||
           CollectParameterSlots(&*spec_.repeatable_seed, slots)) &&
          std::ranges::all_of(spec_.partition_by,
                              [&](Expression& partition) {
                                return CollectParameterSlots(&partition, slots);
                              }) &&
          child_->InstallParameterSlots(parameters, cursor, slots));
}

void SamplePlan::Dump(std::ostream& o, int indent) const {
  o << "Sample: ";
  spec_.Dump(o);
  o << " (estimated rows: " << EmitRowCount() << ")\n"
    << Indent(static_cast<size_t>(indent) + 2);
  DumpChild(o, *child_, indent + 2);
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
    // A child may carry a mix of qualified and unqualified columns; the
    // weight column must pick a real relation qualifier, not blindly take the
    // first column's (which may be a derived, unqualified expression).
    std::string qualifier;
    for (size_t i = 0; i < child.ColumnCount(); ++i) {
      if (!child.GetColumn(i).Name().schema.empty()) {
        qualifier = child.GetColumn(i).Name().schema;
        break;
      }
    }
    columns.emplace_back(ColumnName(qualifier, spec.weight_alias),
                         ValueType::kDouble);
  }
  return {"", std::move(columns)};
}

}  // namespace tinylamb
