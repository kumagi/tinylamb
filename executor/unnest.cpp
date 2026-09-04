/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/unnest.hpp"

#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

UnnestExecutor::UnnestExecutor(TransactionContext& ctx, Schema child_schema,
                               Executor child, Expression unnest_expr,
                               std::string alias, std::string offset_alias)
    : ctx_(ctx),
      child_schema_(std::move(child_schema)),
      child_(std::move(child)),
      unnest_expr_(std::move(unnest_expr)),
      alias_(std::move(alias)),
      offset_alias_(std::move(offset_alias)) {}

bool UnnestExecutor::Next(Row* dst, RowPosition* rp) {
  while (true) {
    if (current_row_index_ < current_unnested_rows_.size()) {
      const Row& unnested_row = current_unnested_rows_[current_row_index_++];
      if (current_child_row_.values_.empty()) {
        *dst = unnested_row;
      } else {
        *dst = current_child_row_ + unnested_row;
      }
      if (rp != nullptr) {
        *rp = RowPosition();
      }
      return true;
    }

    if (child_exhausted_) {
      return false;
    }

    current_child_row_ = Row();
    RowPosition child_rp;
    if (!child_->Next(&current_child_row_, &child_rp)) {
      child_exhausted_ = true;
      return false;
    }

    current_unnested_rows_.clear();
    current_row_index_ = 0;

    relational_detail::Scope scope;
    if (!current_child_row_.values_.empty()) {
      scope.row = &current_child_row_;
      scope.schema = &child_schema_;
    }
    relational_detail::CteMap ctes;

    Value array_val =
        relational_detail::Evaluate(unnest_expr_, scope, nullptr, ctx_, ctes);

    SelectSource source;
    source.alias = alias_.empty() ? "unnest" : alias_;
    source.offset_alias = offset_alias_;
    relational_detail::Relation rel =
        relational_detail::UnnestValueToRelation(source, array_val);
    current_unnested_rows_ = std::move(rel.rows);
  }
}

void UnnestExecutor::Dump(std::ostream& o, int indent) const {
  o << "Unnest: " << (unnest_expr_ ? unnest_expr_->ToString() : "");
  if (!alias_.empty()) {
    o << " AS " << alias_;
  }
  if (!offset_alias_.empty()) {
    o << " WITH OFFSET " << offset_alias_;
  }
  o << "\n";
  if (child_) {
    o << Indent(indent + 2);
    child_->Dump(o, indent + 2);
  }
}

}  // namespace tinylamb
