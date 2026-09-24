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

#include "plan/product_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "executor/hash_join_mode.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "index/index.hpp"
#include "plan/plan.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"

namespace tinylamb {
namespace {

// JoinKind values (executor/join_kind.hpp). The plan layer sees only the
// opaque enum, so comparisons go through the fixed underlying values; the
// executor header owns the names and any future additions. Inner (0) needs
// no constant: it is the complement of the semi/anti/outer predicates.
constexpr uint8_t kJoinKindSemi = 1;
constexpr uint8_t kJoinKindAnti = 2;
constexpr uint8_t kJoinKindNullAwareAnti = 3;
constexpr uint8_t kJoinKindLeftOuter = 4;
constexpr uint8_t kJoinKindRightOuter = 5;
constexpr uint8_t kJoinKindFullOuter = 6;

[[nodiscard]] bool IsSemiOrAnti(JoinKind kind) {
  const auto value = static_cast<uint8_t>(kind);
  return value == kJoinKindSemi || value == kJoinKindAnti ||
         value == kJoinKindNullAwareAnti;
}

[[nodiscard]] bool IsOuter(JoinKind kind) {
  const auto value = static_cast<uint8_t>(kind);
  return value == kJoinKindLeftOuter || value == kJoinKindRightOuter ||
         value == kJoinKindFullOuter;
}

// Row-count statistics feed cost comparisons only; a pathological estimate
// must wrap to "very expensive", never to a small number that would win.
size_t SaturatingMul(size_t a, size_t b) {
  if (a == 0 || b == 0) {
    return 0;
  }
  constexpr size_t kMax = std::numeric_limits<size_t>::max();
  return a > kMax / b ? kMax : a * b;
}

size_t SaturatingAdd(size_t a, size_t b) {
  constexpr size_t kMax = std::numeric_limits<size_t>::max();
  return a > kMax - b ? kMax : a + b;
}

TableStatistics CrossJoinStats(const TableStatistics& left,
                               const TableStatistics& right) {
  TableStatistics ans(left * right.Rows());
  ans.Concat(right * left.Rows());
  return ans;
}

TableStatistics JoinStats(const TableStatistics& left,
                          const TableStatistics& right, JoinKind kind,
                          size_t output_rows) {
  if (IsSemiOrAnti(kind)) {
    return left.ScaleCardinality(output_rows);
  }
  TableStatistics ans = left.ScaleCardinality(output_rows);
  ans.Concat(right.ScaleCardinality(output_rows));
  return ans;
}

size_t EstimatedBuildBytes(const Plan& right_src) {
  return SaturatingMul(right_src->EmitRowCount(), kHashJoinRowBytesEstimate);
}
}  // namespace

size_t EstimatedJoinRowCount(JoinKind kind, size_t left_rows, size_t right_rows,
                             std::optional<size_t> match_rows) {
  const size_t cross = SaturatingMul(left_rows, right_rows);
  const size_t matches = match_rows.value_or(cross);
  if (IsSemiJoinKind(kind)) {
    return std::min(left_rows, matches);
  }
  if (IsAntiJoinKind(kind) || IsNullAwareAntiJoinKind(kind)) {
    return left_rows;
  }
  if (IsLeftOuterJoinKind(kind)) {
    return SaturatingAdd(left_rows, matches);
  }
  if (IsRightOuterJoinKind(kind)) {
    return SaturatingAdd(right_rows, matches);
  }
  if (IsFullOuterJoinKind(kind)) {
    return SaturatingAdd(SaturatingAdd(left_rows, right_rows), matches);
  }
  return matches;
}

// For Hash Join.
ProductPlan::ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
                         Plan right_src, std::vector<ColumnName> right_cols)
    : ProductPlan(std::move(left_src), std::move(left_cols),
                  std::move(right_src), std::move(right_cols),
                  HashJoinMode::kInMemory, std::nullopt) {}

ProductPlan::ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
                         Plan right_src, std::vector<ColumnName> right_cols,
                         HashJoinMode hash_mode,
                         std::optional<size_t> estimated_rows)
    : left_src_(std::move(left_src)),
      right_src_(std::move(right_src)),
      left_cols_(std::move(left_cols)),
      right_cols_(std::move(right_cols)),
      right_tbl_(nullptr),
      right_idx_(nullptr),
      right_ts_(nullptr),
      hash_mode_(hash_mode),
      output_schema_(left_src_->GetSchema() + right_src_->GetSchema()),
      stats_(JoinStats(
          left_src_->GetStats(), right_src_->GetStats(), kind_,
          EstimatedJoinRowCount(kind_, left_src_->EmitRowCount(),
                                right_src_->EmitRowCount(), estimated_rows))) {}

// For Semi/Anti Hash Join: only the left side's schema and rows survive.
ProductPlan::ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
                         Plan right_src, std::vector<ColumnName> right_cols,
                         HashJoinMode hash_mode, JoinKind kind,
                         std::optional<size_t> estimated_rows)
    : left_src_(std::move(left_src)),
      right_src_(std::move(right_src)),
      left_cols_(std::move(left_cols)),
      right_cols_(std::move(right_cols)),
      right_tbl_(nullptr),
      right_idx_(nullptr),
      right_ts_(nullptr),
      hash_mode_(hash_mode),
      kind_(kind),
      output_schema_(IsSemiOrAnti(kind_)
                         ? left_src_->GetSchema()
                         : left_src_->GetSchema() + right_src_->GetSchema()),
      stats_(JoinStats(
          left_src_->GetStats(), right_src_->GetStats(), kind_,
          EstimatedJoinRowCount(kind_, left_src_->EmitRowCount(),
                                right_src_->EmitRowCount(), estimated_rows))) {}

ProductPlan::ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
                         Plan right_src, std::vector<ColumnName> right_cols,
                         JoinKind kind, std::optional<size_t> estimated_rows)
    : ProductPlan(std::move(left_src), std::move(left_cols),
                  std::move(right_src), std::move(right_cols),
                  HashJoinMode::kInMemory, kind, estimated_rows) {}

bool IsSemiJoinKind(JoinKind kind) {
  return static_cast<uint8_t>(kind) == kJoinKindSemi;
}

bool IsAntiJoinKind(JoinKind kind) {
  return static_cast<uint8_t>(kind) == kJoinKindAnti;
}

bool IsNullAwareAntiJoinKind(JoinKind kind) {
  return static_cast<uint8_t>(kind) == kJoinKindNullAwareAnti;
}

bool IsLeftOuterJoinKind(JoinKind kind) {
  return static_cast<uint8_t>(kind) == kJoinKindLeftOuter;
}

bool IsRightOuterJoinKind(JoinKind kind) {
  return static_cast<uint8_t>(kind) == kJoinKindRightOuter;
}

bool IsFullOuterJoinKind(JoinKind kind) {
  return static_cast<uint8_t>(kind) == kJoinKindFullOuter;
}

JoinKind SemiJoinKind() { return static_cast<JoinKind>(kJoinKindSemi); }

JoinKind AntiJoinKind() { return static_cast<JoinKind>(kJoinKindAnti); }

JoinKind NullAwareAntiJoinKind() {
  return static_cast<JoinKind>(kJoinKindNullAwareAnti);
}

JoinKind LeftOuterJoinKind() {
  return static_cast<JoinKind>(kJoinKindLeftOuter);
}

JoinKind RightOuterJoinKind() {
  return static_cast<JoinKind>(kJoinKindRightOuter);
}

JoinKind FullOuterJoinKind() {
  return static_cast<JoinKind>(kJoinKindFullOuter);
}

// For Index Join.
ProductPlan::ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
                         const Table& right_tbl, const Index& idx,
                         std::vector<ColumnName> right_cols,
                         const TableStatistics& right_ts,
                         std::optional<size_t> estimated_rows)
    : left_src_(std::move(left_src)),
      left_cols_(std::move(left_cols)),
      right_cols_(std::move(right_cols)),
      right_tbl_(&right_tbl),
      right_idx_(&idx),
      right_ts_(&right_ts),
      hash_mode_(HashJoinMode::kInMemory),
      output_schema_(left_src_->GetSchema() + right_tbl_->GetSchema()),
      stats_(JoinStats(
          left_src_->GetStats(), *right_ts_, kind_,
          EstimatedJoinRowCount(kind_, left_src_->EmitRowCount(),
                                right_ts_->Rows(), estimated_rows))) {}

// For Index Join over a relation-renamed right side: the emitted columns are
// named by `declared_output` while key offsets still resolve physically.
ProductPlan::ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
                         const Table& right_tbl, const Index& idx,
                         std::vector<ColumnName> right_cols,
                         const TableStatistics& right_ts,
                         Schema declared_output,
                         std::optional<size_t> estimated_rows)
    : left_src_(std::move(left_src)),
      left_cols_(std::move(left_cols)),
      right_cols_(std::move(right_cols)),
      right_tbl_(&right_tbl),
      right_idx_(&idx),
      right_ts_(&right_ts),
      hash_mode_(HashJoinMode::kInMemory),
      output_schema_(std::move(declared_output)),
      stats_(JoinStats(
          left_src_->GetStats(), *right_ts_, kind_,
          EstimatedJoinRowCount(kind_, left_src_->EmitRowCount(),
                                right_ts_->Rows(), estimated_rows))) {}

// For Cross Join.
ProductPlan::ProductPlan(Plan left_src, Plan right_src)
    : left_src_(std::move(left_src)),
      right_src_(std::move(right_src)),
      right_tbl_(nullptr),
      right_idx_(nullptr),
      right_ts_(nullptr),
      hash_mode_(HashJoinMode::kInMemory),
      output_schema_(left_src_->GetSchema() + right_src_->GetSchema()),
      stats_(CrossJoinStats(left_src_->GetStats(), right_src_->GetStats())) {}

ProductPlan::ProductPlan(Plan left_src, Plan right_src, JoinKind kind,
                         std::optional<size_t> estimated_rows)
    : left_src_(std::move(left_src)),
      right_src_(std::move(right_src)),
      right_tbl_(nullptr),
      right_idx_(nullptr),
      right_ts_(nullptr),
      hash_mode_(HashJoinMode::kInMemory),
      kind_(kind),
      output_schema_(left_src_->GetSchema() + right_src_->GetSchema()),
      stats_(
          IsOuter(kind)
              ? JoinStats(left_src_->GetStats(), right_src_->GetStats(), kind_,
                          EstimatedJoinRowCount(
                              kind_, left_src_->EmitRowCount(),
                              right_src_->EmitRowCount(), estimated_rows))
              : CrossJoinStats(left_src_->GetStats(), right_src_->GetStats())) {
}

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp).

[[nodiscard]] const Schema& ProductPlan::GetSchema() const {
  return output_schema_;
}

size_t ProductPlan::AccessRowCount() const {
  if (left_cols_.empty() && right_cols_.empty()) {
    // Cross Join.
    return SaturatingAdd(
        left_src_->AccessRowCount(),
        SaturatingAdd(size_t{1}, SaturatingMul(left_src_->EmitRowCount(),
                                               right_src_->AccessRowCount())));
  }
  if (right_tbl_ != nullptr) {
    // IndexJoin.
    return left_src_->AccessRowCount() * 3;
  }
  const size_t base =
      SaturatingAdd(left_src_->AccessRowCount(), right_src_->AccessRowCount());
  const size_t build_bytes = EstimatedBuildBytes(right_src_);
  if (hash_mode_ == HashJoinMode::kHybrid) {
    // Hybrid pays spill I/O on non-resident partitions (~1.5x scan cost).
    return base + (base / 2);
  }
  // Penalize in-memory hash when the build side clearly will not fit so
  // Cascades prefers HybridHashJoin.
  if (PreferHybridHashJoin(build_bytes)) {
    return SaturatingMul(base, 1000);
  }
  return base;
}

size_t ProductPlan::EmitRowCount() const { return stats_.Rows(); }

bool ProductPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                              const std::vector<bool>& ascending) const {
  return IsOrderedBy(expressions, ascending, {});
}

bool ProductPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  if (!IsSemiOrAnti(kind_)) {
    return false;
  }
  return left_src_->IsOrderedBy(expressions, ascending, nulls_first);
}

void ProductPlan::Dump(std::ostream& o, int indent) const {
  o << "Product: ";
  if (batch_nested_loop_) {
    o << "Batch ";
  }
  if (static_cast<uint8_t>(kind_) == kJoinKindSemi) {
    o << "Semi Join ";
  } else if (static_cast<uint8_t>(kind_) == kJoinKindAnti) {
    o << "Anti Join ";
  } else if (static_cast<uint8_t>(kind_) == kJoinKindNullAwareAnti) {
    o << "Null-aware Anti Join ";
  } else if (static_cast<uint8_t>(kind_) == kJoinKindLeftOuter) {
    o << "Left Outer Join ";
  } else if (static_cast<uint8_t>(kind_) == kJoinKindRightOuter) {
    o << "Right Outer Join ";
  } else if (static_cast<uint8_t>(kind_) == kJoinKindFullOuter) {
    o << "Full Outer Join ";
  } else if (left_cols_.empty() && right_cols_.empty()) {
    o << "Cross Join ";
  } else if (right_tbl_ != nullptr) {
    o << "Index Join ";
  } else if (hash_mode_ == HashJoinMode::kHybrid) {
    o << "Hybrid Hash Join ";
  } else {
    o << "Hash Join ";
  }
  if (!(left_cols_.empty() && right_cols_.empty())) {
    o << "left:{";
    for (size_t i = 0; i < left_cols_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << left_cols_[i];
    }
    o << "} right:{";
    for (size_t i = 0; i < right_cols_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << right_cols_[i];
    }
    o << "} ";
  }
  o << " (estimated rows: " << EmitRowCount() << ")";
  o << "\n" << Indent(static_cast<size_t>(indent) + 2);
  left_src_->Dump(o, indent + 2);
  o << "\n" << Indent(static_cast<size_t>(indent) + 2);
  if (right_tbl_ == nullptr) {
    // CrossJoin or HashJoin.
    right_src_->Dump(o, indent + 2);
  } else {
    // IndexJoin.
    right_idx_->Dump(o);
  }
}

std::string ProductPlan::ToString() const {
  std::string s = "Product: ";
  if (batch_nested_loop_) {
    s += "Batch ";
  }
  if (static_cast<uint8_t>(kind_) == kJoinKindSemi) {
    s += "Semi Join ";
  } else if (static_cast<uint8_t>(kind_) == kJoinKindAnti) {
    s += "Anti Join ";
  } else if (static_cast<uint8_t>(kind_) == kJoinKindNullAwareAnti) {
    s += "Null-aware Anti Join ";
  } else if (static_cast<uint8_t>(kind_) == kJoinKindLeftOuter) {
    s += "Left Outer Join ";
  } else if (static_cast<uint8_t>(kind_) == kJoinKindRightOuter) {
    s += "Right Outer Join ";
  } else if (static_cast<uint8_t>(kind_) == kJoinKindFullOuter) {
    s += "Full Outer Join ";
  } else if (left_cols_.empty() && right_cols_.empty()) {
    s += "Cross Join ";
  } else if (right_tbl_ != nullptr) {
    s += "Index Join ";
  } else if (hash_mode_ == HashJoinMode::kHybrid) {
    s += "Hybrid Hash Join ";
  } else {
    s += "Hash Join ";
  }
  if (!(left_cols_.empty() && right_cols_.empty())) {
    s += "left:{";
    for (size_t i = 0; i < left_cols_.size(); ++i) {
      if (0 < i) {
        s += ", ";
      }
      s += left_cols_[i].ToString();
    }
    s += "} right:{";
    for (size_t i = 0; i < right_cols_.size(); ++i) {
      if (0 < i) {
        s += ", ";
      }
      s += right_cols_[i].ToString();
    }
    s += "} ";
  }
  s += " (estimated rows: " + std::to_string(EmitRowCount()) + ")";
  return s;
}

bool ProductPlan::ContainsParameterSlots() const {
  // Index-join shapes carry their right side as a raw Table (right_src_ null),
  // so only the left subtree and the residual note can hold slots.
  return left_src_->ContainsParameterSlots() ||
         (right_src_ && right_src_->ContainsParameterSlots()) ||
         ContainsParameterSlot(residual_note_);
}

bool ProductPlan::InstallParameterSlots(const std::vector<Value>& parameters,
                                        size_t* cursor,
                                        std::vector<Expression>* slots) {
  return left_src_->InstallParameterSlots(parameters, cursor, slots) &&
         (!right_src_ ||
          right_src_->InstallParameterSlots(parameters, cursor, slots)) &&
         SlotizeInPlace(&residual_note_, parameters, cursor, slots);
}
}  // namespace tinylamb
