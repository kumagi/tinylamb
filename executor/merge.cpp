/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/merge.hpp"

#include <cstddef>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

MergeExecutor::MergeExecutor(
    Transaction& txn, Table* target_table, Executor source,
    Schema source_schema, Expression on_condition,
    std::vector<WhenMatchedClause> matched_clauses,
    std::vector<WhenNotMatchedClause> not_matched_clauses)
    : txn_(&txn),
      target_table_(target_table),
      source_(std::move(source)),
      source_schema_(std::move(source_schema)),
      on_condition_(std::move(on_condition)),
      matched_clauses_(std::move(matched_clauses)),
      not_matched_clauses_(std::move(not_matched_clauses)) {}

bool MergeExecutor::Next(Row* dst, RowPosition* rp) {
  if (executed_) {
    return false;
  }

  struct TargetEntry {
    Row row;
    RowPosition pos;
    bool deleted{false};
    bool updated{false};
    // Rows inserted by an earlier source row of this same MERGE.  Standard
    // MERGE evaluates ON against the pre-statement target snapshot, so these
    // are bookkept but never probed by later source rows.
    bool inserted{false};
  };

  std::vector<TargetEntry> target_entries;
  for (auto it = target_table_->BeginFullScan(*txn_); it.IsValid(); ++it) {
    target_entries.push_back({*it, it.Position(), false, false, false});
  }

  const Schema& target_schema = target_table_->GetSchema();
  const Schema combined_schema = target_schema + source_schema_;

  Row src_row;
  RowPosition src_pos;
  while (source_->Next(&src_row, &src_pos)) {
    bool matched_any = false;
    for (auto& entry : target_entries) {
      if (entry.deleted || entry.inserted) {
        continue;
      }
      bool is_match = false;
      if (!on_condition_) {
        is_match = true;
      } else {
        Row combined = entry.row + src_row;
        StatusOr<Value> res =
            on_condition_->TryEvaluate(combined, combined_schema);
        is_match = res.HasValue() && !res.Value().IsNull() &&
                   res.Value().Truthy();  // Errors are not TRUE: no match.
      }

      if (is_match) {
        matched_any = true;
        Row combined = entry.row + src_row;
        for (const auto& clause : matched_clauses_) {
          if (clause.condition) {
            StatusOr<Value> c_res =
                clause.condition->TryEvaluate(combined, combined_schema);
            if (!c_res.HasValue() || c_res.Value().IsNull() ||
                !c_res.Value().Truthy()) {  // Errors skip the clause.
              continue;
            }
          }

          if (clause.action == WhenMatchedClause::Action::kUpdate) {
            Row updated_row = entry.row;
            for (const auto& [col_idx, expr] : clause.assignments) {
              if (col_idx < updated_row.values_.size() && expr) {
                StatusOr<Value> assigned =
                    expr->TryEvaluate(combined, combined_schema);
                if (!assigned.HasValue()) {
                  return FailWith(assigned.GetStatus());
                }
                updated_row[col_idx] = assigned.MoveValue();
              }
            }
            StatusOr<RowPosition> st =
                target_table_->Update(*txn_, entry.pos, updated_row);
            if (st.GetStatus() != Status::kSuccess) {
              // Keep the underlying code/diagnostic; only add MERGE context.
              const Status original = st.GetStatus();
              std::string message = "MERGE update failed on table " +
                                    std::string(target_schema.Name());
              if (!original.GetMessage().empty()) {
                message += ": " + original.GetMessage();
              }
              return FailWith(
                  StatusError(original.GetCode(), std::move(message)));
            }
            entry.row = std::move(updated_row);
            entry.updated = true;
            ++updated_count_;
            break;
          }
          if (clause.action == WhenMatchedClause::Action::kDelete) {
            Status st = target_table_->Delete(*txn_, entry.pos);
            if (st != Status::kSuccess) {
              const Status& original = st;
              std::string message = "MERGE delete failed on table " +
                                    std::string(target_schema.Name());
              if (!original.GetMessage().empty()) {
                message += ": " + original.GetMessage();
              }
              return FailWith(
                  StatusError(original.GetCode(), std::move(message)));
            }
            entry.deleted = true;
            ++deleted_count_;
            break;
          }
        }
      }
    }

    if (!matched_any) {
      for (const auto& clause : not_matched_clauses_) {
        if (clause.condition) {
          StatusOr<Value> c_res_or =
              clause.condition->TryEvaluate(src_row, source_schema_);
          if (!c_res_or.HasValue() || c_res_or.Value().IsNull() ||
              !c_res_or.Value().Truthy()) {  // Errors skip the clause.
            continue;
          }
        }

        if (clause.action == WhenNotMatchedClause::Action::kInsert) {
          Row inserted_row(std::vector<Value>(target_schema.ColumnCount()));
          if (clause.target_columns.empty()) {
            for (size_t i = 0;
                 i < clause.values.size() && i < inserted_row.values_.size();
                 ++i) {
              if (clause.values[i]) {
                StatusOr<Value> inserted =
                    clause.values[i]->TryEvaluate(src_row, source_schema_);
                if (!inserted.HasValue()) {
                  return FailWith(inserted.GetStatus());
                }
                inserted_row[i] = inserted.MoveValue();
              }
            }
          } else {
            for (size_t i = 0;
                 i < clause.target_columns.size() && i < clause.values.size();
                 ++i) {
              size_t target_col = clause.target_columns[i];
              if (target_col < inserted_row.values_.size() &&
                  clause.values[i]) {
                StatusOr<Value> inserted =
                    clause.values[i]->TryEvaluate(src_row, source_schema_);
                if (!inserted.HasValue()) {
                  return FailWith(inserted.GetStatus());
                }
                inserted_row[target_col] = inserted.MoveValue();
              }
            }
          }
          StatusOr<RowPosition> st = target_table_->Insert(*txn_, inserted_row);
          if (st.GetStatus() != Status::kSuccess) {
            const Status original = st.GetStatus();
            std::string message = "MERGE insert failed on table " +
                                  std::string(target_schema.Name());
            if (!original.GetMessage().empty()) {
              message += ": " + original.GetMessage();
            }
            return FailWith(
                StatusError(original.GetCode(), std::move(message)));
          }
          target_entries.push_back(
              {inserted_row, st.Value(), false, false, true});
          ++inserted_count_;
          break;
        }
      }
    }
  }
  if (source_->GetStatus() != Status::kSuccess) {
    return FailWith(source_->GetStatus());
  }
  FailWithChildOf(*source_);

  *dst = Row({Value("Merge Rows"), Value(inserted_count_),
              Value(updated_count_), Value(deleted_count_)});
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  executed_ = true;
  return true;
}

void MergeExecutor::Dump(std::ostream& o, int indent) const {
  o << "MergeExecutor: " << target_table_->GetSchema().Name() << "\n"
    << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(o, indent + 2);
}

}  // namespace tinylamb
