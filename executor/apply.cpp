/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/apply.hpp"

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/join_kind.hpp"
#include "common/status_or.hpp"
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

bool ApplyExecutor::NextRow(Row* dst, RowPosition* rp) {
  DataChunk outer_batch;
  while (true) {
    if (!has_outer_row_) {
      if (outer_exec_->NextBatch(&outer_batch, 1) == 0) {
        FailWithChildOf(*outer_exec_);
        return false;
      }
      current_outer_row_ = outer_batch.RowAt(0);
      current_outer_rp_ = outer_batch.PositionAt(0);
      has_outer_row_ = true;
      matched_any_ = false;
      inner_rows_.clear();
      inner_idx_ = 0;

      if (inner_query_ != nullptr) {
        relational_detail::Scope scope{.row = &current_outer_row_,
                                       .schema = &outer_schema_,
                                       .outer = nullptr};
        StatusOr<relational_detail::Relation> executed =
            relational_detail::ExecuteQuery(context_, *inner_query_, &scope,
                                            {});
        if (!executed.HasValue()) {
          return FailWith(executed.GetStatus());
        }
        relational_detail::Relation rel = executed.MoveValue();
        if (inner_schema_.ColumnCount() == 0) {
          inner_schema_ =
              inner_alias_.empty()
                  ? rel.schema
                  : relational_detail::QualifySchema(rel.schema, inner_alias_);
          combined_schema_ = outer_schema_ + inner_schema_;
        }
        const Status copied =
            rel.ForEachRow([&](const Row& r) { inner_rows_.push_back(r); });
        if (copied != Status::kSuccess) {
          return FailWith(copied);
        }
        if (rel.GetStatus() != Status::kSuccess) {
          return FailWith(rel.GetStatus());
        }
      } else if (inner_exec_ != nullptr) {
        if (!static_inner_cached_) {
          DataChunk inner_batch;
          while (inner_exec_->NextBatch(&inner_batch) != 0) {
            for (size_t i = 0; i < inner_batch.Size(); ++i) {
              static_inner_rows_.push_back(inner_batch.RowAt(i));
            }
          }
          if (inner_exec_->GetStatus() != Status::kSuccess) {
            return FailWith(inner_exec_->GetStatus());
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
        StatusOr<Value> res =
            predicate_->TryEvaluate(combined, combined_schema_);
        if (!res.HasValue()) {
          return FailWith(res.GetStatus());
        }
        match = !res.Value().IsNull() && res.Value().Truthy();
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

size_t ApplyExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) {
    return 0;
  }
  Row row;
  RowPosition rp;
  while (destination->Size() < max_rows && NextRow(&row, &rp)) {
    destination->Append(std::move(row), rp);
  }
  return destination->Size();
}

void ApplyExecutor::Dump(std::ostream& o, int indent) const {
  o << "ApplyExecutor [kind: " << static_cast<int>(kind_)
    << ", alias: " << inner_alias_ << "]\n";
  if (outer_exec_) {
    outer_exec_->Dump(o, indent + 2);
  }
}

}  // namespace tinylamb
