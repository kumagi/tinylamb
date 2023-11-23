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

#include <iostream>
#include <utility>

#include "database/transaction_context.hpp"
#include "executor/cross_join.hpp"
#include "executor/hash_join.hpp"
#include "executor/index_join.hpp"
#include "table/table.hpp"
#include "type/schema.hpp"

namespace tinylamb {
namespace {

TableStatistics CrossJoinStats(const TableStatistics& left,
                               const TableStatistics& right) {
  TableStatistics ans(left * right.Rows());
  ans.Concat(right * left.Rows());
  return ans;
}

TableStatistics HashJoinStats(const TableStatistics& left,
                              const std::vector<ColumnName>& /*left_cols*/,
                              const TableStatistics& right,
                              const std::vector<ColumnName>& /*right_cols*/) {
  TableStatistics ans(left);
  ans.Concat(right);
  return ans;
}

}  // namespace

// For Hash Join.
ProductPlan::ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
                         Plan right_src, std::vector<ColumnName> right_cols)
    : left_src_(std::move(left_src)),
      right_src_(std::move(right_src)),
      left_cols_(std::move(left_cols)),
      right_cols_(std::move(right_cols)),
      right_tbl_(nullptr),
      right_idx_(nullptr),
      right_ts_(nullptr),
      output_schema_(left_src_->GetSchema() + right_src_->GetSchema()),
      stats_(HashJoinStats(left_src_->GetStats(), left_cols_,
                           right_src_->GetStats(), right_cols_)) {}

// For Index Join.
ProductPlan::ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
                         const Table& right_tbl, const Index& idx,
                         std::vector<ColumnName> right_cols,
                         const TableStatistics& right_ts)
    : left_src_(std::move(left_src)),
      left_cols_(std::move(left_cols)),
      right_cols_(std::move(right_cols)),
      right_tbl_(&right_tbl),
      right_idx_(&idx),
      right_ts_(&right_ts),
      output_schema_(left_src_->GetSchema() + right_tbl_->GetSchema()),
      stats_(HashJoinStats(left_src_->GetStats(), left_cols_, *right_ts_,
                           right_cols_)) {}

// For Cross Join.
ProductPlan::ProductPlan(Plan left_src, Plan right_src)
    : left_src_(std::move(left_src)),
      right_src_(std::move(right_src)),
      right_tbl_(nullptr),
      right_idx_(nullptr),
      right_ts_(nullptr),
      output_schema_(left_src_->GetSchema() + right_src_->GetSchema()),
      stats_(CrossJoinStats(left_src_->GetStats(), right_src_->GetStats())) {}

Executor ProductPlan::EmitExecutor(TransactionContext& ctx) const {
  if (left_cols_.empty() && right_cols_.empty()) {  // Cross Join
    return std::make_shared<CrossJoin>(left_src_->EmitExecutor(ctx),
                                       right_src_->EmitExecutor(ctx));
  }
  std::vector<slot_t> left;
  std::vector<slot_t> right;
  {
    // Build left offsets.
    left.reserve(left_cols_.size());
    const Schema& left_schema = left_src_->GetSchema();
    for (const auto& col : left_cols_) {
      for (size_t i = 0; i < left_schema.ColumnCount(); ++i) {
        const Column c = left_schema.GetColumn(i);
        if (c.Name() == col) {
          left.push_back(i);
        }
      }
    }

    // Build right offsets.
    left.reserve(right_cols_.size());
    const Schema& right_schema = right_tbl_ != nullptr
                                     ? right_tbl_->GetSchema()
                                     : right_src_->GetSchema();
    for (const auto& col : right_cols_) {
      for (size_t i = 0; i < right_schema.ColumnCount(); ++i) {
        const Column c = right_schema.GetColumn(i);
        if (c.Name() == col) {
          right.push_back(i);
        }
      }
    }
  }
  if (right_tbl_ != nullptr) {
    // IndexJoin.
    return std::make_shared<IndexJoin>(ctx.txn_, left_src_->EmitExecutor(ctx),
                                       left, *right_tbl_, *right_idx_, right);
  }
  return std::make_shared<HashJoin>(left_src_->EmitExecutor(ctx), left,
                                    right_src_->EmitExecutor(ctx), right);
}

[[nodiscard]] const Schema& ProductPlan::GetSchema() const {
  return output_schema_;
}

size_t ProductPlan::AccessRowCount() const {
  if (left_cols_.empty() && right_cols_.empty()) {
    return left_src_->AccessRowCount() +
           (1 + left_src_->EmitRowCount() * right_src_->AccessRowCount());
  }
  if (right_tbl_ != nullptr) {
    // IndexJoin.
    return left_src_->AccessRowCount() * 3;
  }
  // Cost of hash join.
  return left_src_->AccessRowCount() + right_src_->AccessRowCount();
}

size_t ProductPlan::EmitRowCount() const {
  if (left_cols_.empty() && right_cols_.empty()) {  // CrossJoin.
    return left_src_->EmitRowCount() * right_src_->EmitRowCount();
  }
  if (right_tbl_ != nullptr) {  // IndexJoin
    return std::min(left_src_->EmitRowCount(), right_ts_->Rows());
  }
  return std::min(left_src_->EmitRowCount(), right_src_->EmitRowCount());
}

void ProductPlan::Dump(std::ostream& o, int indent) const {
  o << "Product: ";
  if (left_cols_.empty() && right_cols_.empty()) {
    o << "Cross Join ";
  } else {
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
  o << " (estimated cost: " << EmitRowCount() << ")";
  o << "\n" << Indent(indent + 2);
  left_src_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  if (right_tbl_ == nullptr) {
    // CrossJoin or HashJoin.
    right_src_->Dump(o, indent + 2);
  } else {
    // IndexJoin.
    right_idx_->Dump(o);
  }
}

}  // namespace tinylamb