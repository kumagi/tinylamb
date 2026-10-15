/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/late_materialization_join_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "common/late_materialization.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "index/index.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "table/table.hpp"
#include "type/schema.hpp"

namespace tinylamb {
namespace {

size_t SaturatingAdd(size_t left, size_t right) {
  constexpr size_t kMax = std::numeric_limits<size_t>::max();
  return left > kMax - right ? kMax : left + right;
}

}  // namespace

LateMaterializationJoinPlan::LateMaterializationJoinPlan(
    Plan left, std::vector<ColumnName> left_keys, const Table& right_table,
    const Index& right_index, std::vector<ColumnName> right_keys,
    const TableStatistics& right_statistics, Expression predicate,
    Schema declared_output, std::vector<slot_t> right_projection,
    bool unique_winner, std::optional<size_t> estimated_rows)
    : left_(std::move(left)),
      right_table_(&right_table),
      right_index_(&right_index),
      left_keys_(std::move(left_keys)),
      right_keys_(std::move(right_keys)),
      right_projection_(std::move(right_projection)),
      predicate_(std::move(predicate)),
      identity_(unique_winner ? LateMaterializationIdentity::kUniqueWinner
                              : LateMaterializationIdentity::kEquiKey),
      unique_winner_(unique_winner),
      right_row_count_(right_statistics.Rows()),
      schema_(declared_output.ColumnCount() == 0
                  ? left_->GetSchema() + right_table.GetSchema()
                  : std::move(declared_output)),
      stats_(left_->GetStats().ScaleCardinality(estimated_rows.value_or(
          EstimatedJoinRowCount(JoinKind::kInner, left_->GetStats().Rows(),
                                right_statistics.Rows())))) {
  CHECK_MSG(left_ != nullptr,
            "LateMaterializationJoinPlan requires a left plan");
  CHECK_MSG(schema_.ColumnCount() > left_->GetSchema().ColumnCount(),
            "LateMaterializationJoinPlan requires a payload projection");
  const bool keys_aligned =
      left_keys_.size() == right_keys_.size() && !left_keys_.empty();
  CHECK_MSG(keys_aligned,
            "LateMaterializationJoinPlan requires aligned non-empty keys");
  CHECK_MSG(right_projection_.size() <= right_table.GetSchema().ColumnCount(),
            "LateMaterializationJoinPlan payload projection is too wide");
  for (const slot_t column : right_projection_) {
    CHECK_MSG(
        column < right_table.GetSchema().ColumnCount(),
        "LateMaterializationJoinPlan payload column is outside the table");
  }
  InitializeProjection(schema_);
}

LateMaterializationJoinPlan::LateMaterializationJoinPlan(
    Plan left, const Table& right_table, Schema declared_output,
    std::vector<slot_t> right_projection, LateMaterializationRowIdBinding rowid,
    Expression predicate, std::optional<size_t> estimated_rows)
    : left_(std::move(left)),
      right_table_(&right_table),
      right_projection_(std::move(right_projection)),
      predicate_(std::move(predicate)),
      identity_(LateMaterializationIdentity::kRowId),
      rowid_(rowid),
      right_row_count_(0),
      schema_(declared_output.ColumnCount() == 0
                  ? left_->GetSchema() + right_table.GetSchema()
                  : std::move(declared_output)),
      stats_(left_->GetStats().ScaleCardinality(
          estimated_rows.value_or(left_->GetStats().Rows()))) {
  CHECK_MSG(left_ != nullptr,
            "LateMaterializationJoinPlan requires a left plan");
  CHECK_MSG(schema_.ColumnCount() > left_->GetSchema().ColumnCount(),
            "LateMaterializationJoinPlan requires a payload projection");
  CHECK_MSG(right_projection_.size() <= right_table.GetSchema().ColumnCount(),
            "LateMaterializationJoinPlan payload projection is too wide");
  for (const slot_t column : right_projection_) {
    CHECK_MSG(
        column < right_table.GetSchema().ColumnCount(),
        "LateMaterializationJoinPlan payload column is outside the table");
  }
  InitializeProjection(schema_);
}

void LateMaterializationJoinPlan::InitializeProjection(const Schema& declared) {
  if (right_table_ == nullptr || !right_projection_.empty()) {
    return;
  }
  const size_t left_width = left_->GetSchema().ColumnCount();
  std::vector<slot_t> derived;
  if (declared.ColumnCount() >= left_width) {
    derived.reserve(declared.ColumnCount() - left_width);
    for (size_t i = left_width; i < declared.ColumnCount(); ++i) {
      const int offset = right_table_->GetSchema().Offset(
          ColumnName("", declared.GetColumn(i).Name().name));
      if (offset < 0) {
        derived.clear();
        break;
      }
      derived.push_back(static_cast<slot_t>(offset));
    }
  }
  if (derived.empty()) {
    derived.resize(right_table_->GetSchema().ColumnCount());
    for (size_t i = 0; i < derived.size(); ++i) {
      derived[i] = static_cast<slot_t>(i);
    }
  }
  right_projection_ = std::move(derived);
}

size_t LateMaterializationJoinPlan::AccessRowCount() const {
  return SaturatingAdd(SaturatingAdd(left_->AccessRowCount(), right_row_count_),
                       stats_.Rows());
}

bool LateMaterializationJoinPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending) const {
  if (expressions.size() != ascending.size() ||
      expressions.size() > left_keys_.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (!expressions[i] || !ascending[i] ||
        expressions[i]->Type() != TypeTag::kColumnValue ||
        expressions[i]->AsColumnValue().GetColumnName() != left_keys_[i]) {
      return false;
    }
  }
  return left_->IsOrderedBy(expressions, ascending);
}

bool LateMaterializationJoinPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  if (expressions.size() != ascending.size() ||
      expressions.size() > left_keys_.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (!expressions[i] || !ascending[i] ||
        expressions[i]->Type() != TypeTag::kColumnValue ||
        expressions[i]->AsColumnValue().GetColumnName() != left_keys_[i]) {
      return false;
    }
  }
  return left_->IsOrderedBy(expressions, ascending, nulls_first);
}

void LateMaterializationJoinPlan::Dump(std::ostream& output, int indent) const {
  output << ToString() << "\n" << Indent(static_cast<size_t>(indent) + 2);
  left_->Dump(output, indent + 2);
  if (right_table_ != nullptr) {
    output << "\n"
           << Indent(static_cast<size_t>(indent) + 2)
           << "LateMaterializePayload: " << right_table_->GetSchema().Name();
    if (right_index_ != nullptr) {
      output << " via " << right_index_->sc_.name_;
    }
  }
}

std::string LateMaterializationJoinPlan::ToString() const {
  std::string result = "LateMaterializationJoin (identity=";
  result += std::string(tinylamb::ToString(identity_));
  result += ", payload=late, keys=" +
            std::to_string(identity_ == LateMaterializationIdentity::kRowId
                               ? size_t{1}
                               : right_keys_.size());
  result += ")";
  return result;
}

bool LateMaterializationJoinPlan::ContainsParameterSlots() const {
  return left_->ContainsParameterSlots() || ContainsParameterSlot(predicate_);
}

bool LateMaterializationJoinPlan::InstallParameterSlots(
    const std::vector<Value>& parameters, size_t* cursor,
    std::vector<Expression>* slots) {
  return left_->InstallParameterSlots(parameters, cursor, slots) &&
         SlotizeInPlace(&predicate_, parameters, cursor, slots);
}

}  // namespace tinylamb
