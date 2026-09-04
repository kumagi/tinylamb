/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/recursive_cte.hpp"

#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "database/transaction_context.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "page/row_position.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"

namespace tinylamb {

RecursiveCteExecutor::RecursiveCteExecutor(
    TransactionContext& context, std::string cte_name,
    std::shared_ptr<const SelectStatement> body,
    std::optional<RecursiveDepthSpec> depth_spec, Schema output_schema)
    : context_(context),
      cte_name_(std::move(cte_name)),
      body_(std::move(body)),
      depth_spec_(std::move(depth_spec)),
      output_schema_(std::move(output_schema)) {}

void RecursiveCteExecutor::Initialize() {
  if (initialized_) {
    return;
  }
  initialized_ = true;
  if (!body_) {
    return;
  }
  const RecursiveDepthSpec* depth_spec =
      depth_spec_.has_value() ? &depth_spec_.value() : nullptr;
  relational_detail::Relation rel = relational_detail::ExecuteRecursiveCte(
      context_, cte_name_, *body_, nullptr, {}, depth_spec);
  rel.ForEachRow([&](const Row& r) { rows_.push_back(r); });
}

bool RecursiveCteExecutor::Next(Row* dst, RowPosition* rp) {
  if (!initialized_) {
    Initialize();
  }
  if (row_idx_ >= rows_.size()) {
    return false;
  }
  *dst = rows_[row_idx_++];
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  return true;
}

void RecursiveCteExecutor::Dump(std::ostream& o, int indent) const {
  (void)indent;
  o << "RecursiveCteExecutor [" << cte_name_ << "]\n";
}

}  // namespace tinylamb
