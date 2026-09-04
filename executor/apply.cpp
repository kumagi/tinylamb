/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/apply.hpp"

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/join_kind.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/relational.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

ApplyExecutor::ApplyExecutor(TransactionContext& context, Schema outer_schema,
                             Executor outer_exec,
                             std::shared_ptr<const SelectStatement> inner_query,
                             std::string inner_alias, Executor inner_exec,
                             Expression predicate, JoinKind kind,
                             Schema output_schema)
    : context_(context),
      outer_schema_(std::move(outer_schema)),
      outer_exec_(std::move(outer_exec)),
      inner_query_(std::move(inner_query)),
      inner_alias_(std::move(inner_alias)),
      inner_exec_(std::move(inner_exec)),
      predicate_(std::move(predicate)),
      kind_(kind),
      output_schema_(std::move(output_schema)) {}

bool ApplyExecutor::Next(Row* dst, RowPosition* rp) {
  while (true) {
    if (!has_outer_row_) {
      if (!outer_exec_->Next(&current_outer_row_, &current_outer_rp_)) {
        return false;
      }
      has_outer_row_ = true;
      matched_any_ = false;
      inner_rows_.clear();
      inner_idx_ = 0;

      if (inner_query_ != nullptr) {
        relational_detail::Scope scope{.row = &current_outer_row_,
                                       .schema = &outer_schema_,
                                       .outer = nullptr};
        relational_detail::Relation rel = relational_detail::ExecuteQuery(
            context_, *inner_query_, &scope, {});
        if (inner_schema_.ColumnCount() == 0) {
          inner_schema_ =
              inner_alias_.empty()
                  ? rel.schema
                  : relational_detail::QualifySchema(rel.schema, inner_alias_);
          combined_schema_ = outer_schema_ + inner_schema_;
        }
        rel.ForEachRow([&](const Row& r) { inner_rows_.push_back(r); });
      } else if (inner_exec_ != nullptr) {
        if (!static_inner_cached_) {
          Row r;
          RowPosition p;
          while (inner_exec_->Next(&r, &p)) {
            static_inner_rows_.push_back(std::move(r));
          }
          static_inner_cached_ = true;
        }
        inner_rows_ = static_inner_rows_;
      }
    }

    while (inner_idx_ < inner_rows_.size()) {
      const Row& inner_row = inner_rows_[inner_idx_++];
      Row combined = current_outer_row_ + inner_row;
      bool match = true;
      if (predicate_) {
        Value res = predicate_->Evaluate(combined, combined_schema_);
        match = !res.IsNull() && res.Truthy();
      }
      if (match) {
        matched_any_ = true;
        if (kind_ == JoinKind::kSemi) {
          *dst = current_outer_row_;
          if (rp != nullptr) {
            *rp = current_outer_rp_;
          }
          has_outer_row_ = false;
          return true;
        }
        if (kind_ == JoinKind::kAnti) {
          has_outer_row_ = false;
          break;
        }
        *dst = std::move(combined);
        if (rp != nullptr) {
          *rp = current_outer_rp_;
        }
        return true;
      }
    }

    if (has_outer_row_ && inner_idx_ >= inner_rows_.size()) {
      has_outer_row_ = false;
      if (!matched_any_) {
        if (kind_ == JoinKind::kLeftOuter) {
          size_t inner_cols = inner_schema_.ColumnCount();
          if (inner_cols == 0 && output_schema_.ColumnCount() >
                                     current_outer_row_.values_.size()) {
            inner_cols = output_schema_.ColumnCount() -
                         current_outer_row_.values_.size();
          }
          std::vector<Value> nulls(inner_cols, Value());
          *dst = current_outer_row_ + Row(std::move(nulls));
          if (rp != nullptr) {
            *rp = current_outer_rp_;
          }
          return true;
        }
        if (kind_ == JoinKind::kAnti) {
          *dst = current_outer_row_;
          if (rp != nullptr) {
            *rp = current_outer_rp_;
          }
          return true;
        }
      }
    }
  }
}

void ApplyExecutor::Dump(std::ostream& o, int indent) const {
  o << "ApplyExecutor [kind: " << static_cast<int>(kind_)
    << ", alias: " << inner_alias_ << "]\n";
  if (outer_exec_) {
    outer_exec_->Dump(o, indent + 2);
  }
}

}  // namespace tinylamb
