/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/merge.hpp"

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
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
  };

  std::vector<TargetEntry> target_entries;
  for (auto it = target_table_->BeginFullScan(*txn_); it.IsValid(); ++it) {
    target_entries.push_back({*it, it.Position(), false, false});
  }

  const Schema& target_schema = target_table_->GetSchema();
  const Schema combined_schema = target_schema + source_schema_;

  Row src_row;
  RowPosition src_pos;
  while (source_->Next(&src_row, &src_pos)) {
    bool matched_any = false;
    for (auto& entry : target_entries) {
      if (entry.deleted) {
        continue;
      }
      bool is_match = false;
      if (!on_condition_) {
        is_match = true;
      } else {
        Row combined = entry.row + src_row;
        try {
          Value res = on_condition_->Evaluate(combined, combined_schema);
          is_match = !res.IsNull() && res.Truthy();
        } catch (...) {
          is_match = false;
        }
      }

      if (is_match) {
        matched_any = true;
        Row combined = entry.row + src_row;
        for (const auto& clause : matched_clauses_) {
          if (clause.condition) {
            try {
              Value c_res =
                  clause.condition->Evaluate(combined, combined_schema);
              if (c_res.IsNull() || !c_res.Truthy()) {
                continue;
              }
            } catch (...) {
              continue;
            }
          }

          if (clause.action == WhenMatchedClause::Action::kUpdate) {
            Row updated_row = entry.row;
            for (const auto& [col_idx, expr] : clause.assignments) {
              if (col_idx < updated_row.values_.size() && expr) {
                updated_row[col_idx] =
                    expr->Evaluate(combined, combined_schema);
              }
            }
            StatusOr<RowPosition> st =
                target_table_->Update(*txn_, entry.pos, updated_row);
            if (st.GetStatus() != Status::kSuccess) {
              throw std::runtime_error("MERGE update failed on table " +
                                       std::string(target_schema.Name()));
            }
            entry.row = std::move(updated_row);
            entry.updated = true;
            ++updated_count_;
            break;
          } else if (clause.action == WhenMatchedClause::Action::kDelete) {
            Status st = target_table_->Delete(*txn_, entry.pos);
            if (st != Status::kSuccess) {
              throw std::runtime_error("MERGE delete failed on table " +
                                       std::string(target_schema.Name()));
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
          try {
            Value c_res = clause.condition->Evaluate(src_row, source_schema_);
            if (c_res.IsNull() || !c_res.Truthy()) {
              continue;
            }
          } catch (...) {
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
                inserted_row[i] =
                    clause.values[i]->Evaluate(src_row, source_schema_);
              }
            }
          } else {
            for (size_t i = 0;
                 i < clause.target_columns.size() && i < clause.values.size();
                 ++i) {
              size_t target_col = clause.target_columns[i];
              if (target_col < inserted_row.values_.size() &&
                  clause.values[i]) {
                inserted_row[target_col] =
                    clause.values[i]->Evaluate(src_row, source_schema_);
              }
            }
          }
          StatusOr<RowPosition> st = target_table_->Insert(*txn_, inserted_row);
          if (st.GetStatus() != Status::kSuccess) {
            throw std::runtime_error("MERGE insert failed on table " +
                                     std::string(target_schema.Name()));
          }
          target_entries.push_back({inserted_row, st.Value(), false, false});
          ++inserted_count_;
          break;
        }
      }
    }
  }

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
    << Indent(indent + 2);
  source_->Dump(o, indent + 2);
}

}  // namespace tinylamb
