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

// Resolves a (possibly relation-aliased) column name against `schema`,
// ignoring the qualifier. Returns nullptr when the column is absent.
const Column* ResolveUnqualified(const Schema& schema,
                                 const ColumnName& column) {
  const int offset = schema.Offset(ColumnName("", column.name));
  if (offset < 0) {
    return nullptr;
  }
  return &schema.GetColumn(static_cast<size_t>(offset));
}
}  // namespace

bool SupportsLookupRangeType(ValueType type) {
  return type == ValueType::kInt64 || type == ValueType::kDate;
}

std::string ProductPlan::LookupProbeText() const {
  if (!lookup_seek_) {
    return {};
  }
  std::string text = "probe:{";
  for (size_t i = 0; i < lookup_seek_->right_columns.size(); ++i) {
    if (0 < i) {
      text += ", ";
    }
    if (lookup_seek_->range_position && i == *lookup_seek_->range_position) {
      // `ProductPlan::ToString` hides the namespace-scope overload inside this
      // member, so qualify it explicitly.
      text += lookup_seek_->left_columns[i].ToString() + " " +
              std::string(tinylamb::ToString(lookup_seek_->range_operation)) +
              " " + lookup_seek_->right_columns[i].ToString();
      continue;
    }
    text += lookup_seek_->right_columns[i].ToString();
  }
  text += "} ";
  return text;
}

std::optional<LookupSeek> DeriveLookupSeek(
    const Index& index, const Schema& right_schema,
    const std::vector<ColumnName>& right_columns,
    const std::vector<ColumnName>& left_columns) {
  if (right_columns.size() != left_columns.size()) {
    // Mismatched key vectors cannot be aligned onto the index key order; the
    // relational factory still emits the join, and CHECK_MSG there reports
    // the invariant violation instead of this layer silently truncating.
    return std::nullopt;
  }
  LookupSeek seek;
  // The leading index key columns the equi-join pins, walked in the index's
  // own order. Stopping at the first unpinned column is what keeps the seek a
  // key prefix: anything after the gap is ordered by a value the probe does
  // not fix, so those columns are the residual predicate's business.
  for (const slot_t key_offset : index.sc_.key_) {
    if (key_offset >= right_schema.ColumnCount()) {
      break;
    }
    const ColumnName& key_column = right_schema.GetColumn(key_offset).Name();
    const auto found = std::ranges::find(right_columns, key_column);
    if (found == right_columns.end()) {
      break;
    }
    seek.left_columns.push_back(
        left_columns[static_cast<size_t>(found - right_columns.begin())]);
    seek.right_columns.push_back(key_column);
  }
  if (seek.right_columns.empty()) {
    // No leading index key column is a join key: a point probe is impossible
    // and the factory falls back to the hash join over a full scan.
    return std::nullopt;
  }
  return seek;
}

void ProductPlan::DeriveLookupSeek() {
  if (right_tbl_ == nullptr || right_idx_ == nullptr) {
    lookup_seek_.reset();
    return;
  }
  lookup_seek_ = tinylamb::DeriveLookupSeek(
      *right_idx_, right_tbl_->GetSchema(), right_cols_, left_cols_);
}

bool ProductPlan::SetLookupRange(size_t range_key_offset,
                                 ColumnName left_column,
                                 ColumnName right_column,
                                 BinaryOperation operation) {
  if (!lookup_seek_ || right_tbl_ == nullptr || right_idx_ == nullptr) {
    return false;
  }
  // Only the first unpinned index key column can narrow the seek: a bound on a
  // later column would be ordered behind a value the probe does not fix, so
  // it cannot open a smaller key range than the equality prefix already does.
  if (range_key_offset != lookup_seek_->right_columns.size() ||
      range_key_offset >= right_idx_->sc_.key_.size()) {
    return false;
  }
  const Schema& physical = right_tbl_->GetSchema();
  const Column* right = ResolveUnqualified(physical, right_column);
  const Column* left = ResolveUnqualified(left_src_->GetSchema(), left_column);
  if (right == nullptr || left == nullptr) {
    return false;
  }
  // The range column has to BE the index key column at that offset; a bound on
  // some other column of the same table would order the seek by a value the
  // index does not store there.
  if (right->Name() !=
      physical.GetColumn(right_idx_->sc_.key_[range_key_offset]).Name()) {
    return false;
  }
  // The executor re-checks the comparison on every candidate row. Mismatched
  // operand types make that re-check fail for rows the join predicate matches
  // (a cross-typed comparison is an error, not FALSE), so the range may only
  // be adopted when both sides carry the same whitelisted ordering.
  if (left->Type() != right->Type() ||
      !SupportsLookupRangeType(right->Type())) {
    return false;
  }
  lookup_seek_->left_columns.push_back(std::move(left_column));
  lookup_seek_->right_columns.push_back(std::move(right_column));
  lookup_seek_->range_position = lookup_seek_->left_columns.size() - 1;
  lookup_seek_->range_operation = operation;
  return true;
}

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
    return std::max(left_rows, matches);
  }
  if (IsRightOuterJoinKind(kind)) {
    return std::max(right_rows, matches);
  }
  if (IsFullOuterJoinKind(kind)) {
    const size_t matched_left = std::min(left_rows, matches);
    const size_t matched_right = std::min(right_rows, matches);
    const size_t paired = SaturatingAdd(
        matched_left,
        SaturatingAdd(left_rows - matched_left, right_rows - matched_right));
    return std::max(matches, paired);
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

double JoinKeyDomainEstimate(const Plan& plan, const ColumnName& column) {
  if (plan == nullptr) {
    return 0.0;
  }
  const int offset = plan->GetSchema().Offset(column);
  if (offset < 0 || static_cast<size_t>(offset) >= plan->GetStats().Columns()) {
    return 0.0;
  }
  return static_cast<double>(
      plan->GetStats().Column(static_cast<size_t>(offset)).Distinct());
}

double JoinKeyDomainEstimate(const Plan& plan,
                             const std::vector<ColumnName>& columns) {
  double domain = 0.0;
  for (const ColumnName& column : columns) {
    domain = std::max(domain, JoinKeyDomainEstimate(plan, column));
  }
  return domain;
}

double SemiReductionKeptRows(double probe_rows, double build_rows,
                             double probe_domain, double build_domain) {
  // A build row survives only when the probe side carries its key, so the
  // expected survivor count is the build side scaled by the fraction of the
  // build key domain the probe covers. Without statistics the honest bound is
  // the probe row count (its distinct keys cannot exceed its rows), which is
  // exactly the ceiling the reduction can reach.
  const double domain = probe_domain >= 1.0 ? probe_domain : probe_rows;
  if (build_domain >= 1.0) {
    return build_rows * std::min(1.0, domain / build_domain);
  }
  return std::min(build_rows, domain);
}

bool SemiReductionIsProfitable(double probe_rows, double build_rows,
                               double probe_domain, double build_domain) {
  if (!(probe_rows >= 1.0) || !(build_rows > probe_rows)) {
    return false;
  }
  const double kept =
      SemiReductionKeptRows(probe_rows, build_rows, probe_domain, build_domain);
  // At least half the build side goes away, and enough rows go away to dwarf
  // the two extra hash operations per surviving / pruned row.
  return kept * 2.0 <= build_rows && (build_rows - kept) >= 64.0;
}

bool SemiReductionProfitable(const Plan& probe,
                             const std::vector<ColumnName>& probe_keys,
                             const Plan& build,
                             const std::vector<ColumnName>& build_keys) {
  if (probe == nullptr || build == nullptr) {
    return false;
  }
  return SemiReductionIsProfitable(static_cast<double>(probe->EmitRowCount()),
                                   static_cast<double>(build->EmitRowCount()),
                                   JoinKeyDomainEstimate(probe, probe_keys),
                                   JoinKeyDomainEstimate(build, build_keys));
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
      stats_(
          JoinStats(left_src_->GetStats(), *right_ts_, kind_,
                    EstimatedJoinRowCount(kind_, left_src_->EmitRowCount(),
                                          right_ts_->Rows(), estimated_rows))) {
  DeriveLookupSeek();
}

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
      stats_(
          JoinStats(left_src_->GetStats(), *right_ts_, kind_,
                    EstimatedJoinRowCount(kind_, left_src_->EmitRowCount(),
                                          right_ts_->Rows(), estimated_rows))) {
  DeriveLookupSeek();
}

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
  o << LookupProbeText();
  if (semi_reduction_) {
    o << "semi_reduction ";
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
  s += LookupProbeText();
  if (dynamic_filter_) {
    s += "dynamic_filter:build=";
    s += dynamic_filter_build_left_ ? "left" : "right";
    s += " ";
  }
  if (semi_reduction_) {
    s += "semi_reduction ";
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
