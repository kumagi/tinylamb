/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/window.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {

WindowExecutor::WindowExecutor(TransactionContext& context, Executor child,
                               Schema schema,
                               std::vector<NamedExpression> outputs)
    : WindowExecutor(context, std::move(child), std::move(schema),
                     std::move(outputs), Options{}) {}

WindowExecutor::WindowExecutor(TransactionContext& context, Executor child,
                               Schema schema,
                               std::vector<NamedExpression> outputs,
                               Options options)
    : context_(context),
      child_(std::move(child)),
      schema_(std::move(schema)),
      outputs_(std::move(outputs)),
      options_(options) {
  // The spill shape may only split the input where every call agrees, because
  // a row's file is chosen from that one key: a partition of a call with a
  // different PARTITION BY could otherwise be cut in half.
  spill_enabled_ = options_.memory_budget_bytes != 0 &&
                   options_.spill_partition_count > 0 && child_ != nullptr &&
                   SharedPartitionKey() != nullptr;
}

Schema WindowExecutor::InputSchema() const {
  // The output schema appends one column per call; strip those suffix columns
  // to recover the schema the calls resolve against.
  const size_t input_width = schema_.ColumnCount() >= outputs_.size()
                                 ? schema_.ColumnCount() - outputs_.size()
                                 : 0;
  std::vector<Column> input_columns;
  input_columns.reserve(input_width);
  for (size_t i = 0; i < input_width; ++i) {
    input_columns.push_back(schema_.GetColumn(i));
  }
  return Schema(schema_.Name(), std::move(input_columns));
}

const std::vector<Expression>* WindowExecutor::SharedPartitionKey() const {
  const std::vector<Expression>* shared = nullptr;
  for (const NamedExpression& item : outputs_) {
    if (item.expression == nullptr ||
        item.expression->Type() != TypeTag::kWindowFunctionExp) {
      return nullptr;
    }
    const std::vector<Expression>& partition_by =
        item.expression->AsWindowFunctionCallExpression().partition_by;
    if (shared == nullptr) {
      shared = &partition_by;
      continue;
    }
    if (partition_by.size() != shared->size()) {
      return nullptr;
    }
    for (size_t i = 0; i < partition_by.size(); ++i) {
      // Same expression list, not merely the same width: two calls may only
      // share the spill key if their partitions are the same sets of rows.
      const std::string left = partition_by[i] == nullptr
                                   ? std::string()
                                   : partition_by[i]->ToString();
      const std::string right =
          (*shared)[i] == nullptr ? std::string() : (*shared)[i]->ToString();
      if (left != right) {
        return nullptr;
      }
    }
  }
  return shared;
}

bool WindowExecutor::ComputeColumns() {
  const Schema input_schema = InputSchema();
  const relational_detail::CteMap empty_ctes;
  for (size_t call_index = appended_columns_; call_index < outputs_.size();
       ++call_index) {
    const NamedExpression& item = outputs_[call_index];
    if (item.expression == nullptr ||
        item.expression->Type() != TypeTag::kWindowFunctionExp) {
      return FailWith(
          Status(StatusCode::kUnknown, "window output is not a window call"));
    }
    const WindowFunctionCallExpression& call =
        item.expression->AsWindowFunctionCallExpression();
    std::vector<Value> computed;
    Status status = relational_detail::ComputeOneWindow(
        context_, call, rows_, input_schema, nullptr, empty_ctes, &computed);
    if (status != Status::kSuccess) {
      return FailWith(status);
    }
    if (computed.size() != rows_.size()) {
      return FailWith(
          Status(StatusCode::kUnknown, "window output width mismatch"));
    }
    for (size_t i = 0; i < rows_.size(); ++i) {
      rows_[i].values_.push_back(std::move(computed[i]));
    }
    ++appended_columns_;
  }
  return true;
}

bool WindowExecutor::EnsureComputed() {
  if (computed_) {
    return true;
  }
  if (child_ == nullptr) {
    computed_ = true;
    return true;
  }
  // Drain first: window functions observe whole partitions, so evaluation
  // cannot stream. A child that fails mid-drain leaves nothing behind. A
  // retry after a window-call failure skips the drain: the child is already
  // exhausted and rows_ holds the partially extended input.
  if (appended_columns_ == 0) {
    std::vector<Row> input;
    DataChunk batch;
    while (child_->NextBatch(&batch) != 0) {
      for (size_t i = 0; i < batch.Size(); ++i) {
        input.push_back(batch.RowAt(i));
      }
    }
    if (child_->GetStatus() != Status::kSuccess) {
      FailWithChildOf(*child_);
      return false;
    }
    rows_.reserve(input.size());
    for (Row& input_row : input) {
      rows_.push_back(std::move(input_row));
    }
  }
  if (!ComputeColumns()) {
    return false;
  }
  // Only mark computed after every call succeeded: a later Next() must not
  // replay the partially extended rows_ (they would be missing output
  // columns).
  computed_ = true;
  return true;
}

bool WindowExecutor::DrainIntoSpills(
    const std::vector<Expression>& partition_by) {
  const size_t files = std::max<size_t>(1, options_.spill_partition_count);
  spills_.resize(files);
  const Schema input_schema = InputSchema();
  const relational_detail::CteMap empty_ctes;
  DataChunk batch;
  while (child_->NextBatch(&batch) != 0) {
    for (size_t i = 0; i < batch.Size(); ++i) {
      const Row row = batch.RowAt(i);
      std::vector<Value> key;
      key.reserve(partition_by.size());
      relational_detail::Scope scope;
      scope.row = &row;
      scope.schema = &input_schema;
      for (const Expression& term : partition_by) {
        StatusOr<Value> value = relational_detail::TryEvaluate(
            term, scope, nullptr, context_, empty_ctes);
        if (!value.HasValue()) {
          FailWith(value.GetStatus());
          return false;
        }
        key.push_back(value.MoveValue());
      }
      Status appended =
          spills_[relational_detail::WindowPartitionKeyHash(key) % files]
              .Append(row);
      if (appended != Status::kSuccess) {
        FailWith(appended);
        return false;
      }
    }
  }
  if (child_->GetStatus() != Status::kSuccess) {
    FailWithChildOf(*child_);
    return false;
  }
  for (SpillFile& file : spills_) {
    if (file.Empty()) {
      continue;
    }
    Status finished = file.FinishWriting();
    if (finished != Status::kSuccess) {
      FailWith(finished);
      return false;
    }
    ++spill_file_count_;
  }
  return true;
}

bool WindowExecutor::NextChunk() {
  // A false return must be sticky: callers are allowed to keep pulling after
  // the end (a materializing parent retries with an empty batch), and rewinding
  // read_offset_ here would replay the chunk that is still in rows_ forever.
  if (exhausted_) {
    return false;
  }
  if (!spill_enabled_) {
    if (buffered_done_) {
      exhausted_ = true;
      return false;
    }
    if (!EnsureComputed()) {
      // Leave buffered_done_ false: EnsureComputed's documented retry path
      // resumes from appended_columns_ instead of duplicating columns.
      return false;
    }
    buffered_done_ = true;
    if (rows_.empty()) {
      exhausted_ = true;
      return false;
    }
    read_offset_ = 0;
    return true;
  }
  // Partitioned spill: every partition is whole inside one file, so each file
  // is self-contained. One file resident at a time is the memory bound.
  if (!drained_) {
    const std::vector<Expression>* partition_by = SharedPartitionKey();
    if (partition_by == nullptr || !DrainIntoSpills(*partition_by)) {
      // A file choice needs the shared key; without it there is no safe split.
      drained_ = true;
      next_spill_ = spills_.size();
      rows_.clear();
      exhausted_ = true;
      return false;
    }
    drained_ = true;
  }
  while (next_spill_ < spills_.size()) {
    SpillFile& file = spills_[next_spill_++];
    if (file.Empty()) {
      continue;
    }
    StatusOr<std::vector<Row>> chunk = file.ReadAllRows();
    if (!chunk.HasValue()) {
      FailWith(chunk.GetStatus());
      next_spill_ = spills_.size();
      exhausted_ = true;
      return false;
    }
    rows_ = chunk.MoveValue();
    if (rows_.empty()) {
      continue;
    }
    appended_columns_ = 0;
    peak_resident_rows_ = std::max(peak_resident_rows_, rows_.size());
    if (!ComputeColumns()) {
      // A failure poisons the chunk: do not let a later Next() skip to the
      // next file and emit rows after an error.
      next_spill_ = spills_.size();
      rows_.clear();
      exhausted_ = true;
      return false;
    }
    read_offset_ = 0;
    return true;
  }
  exhausted_ = true;
  return false;
}

size_t WindowExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  if (read_offset_ >= rows_.size() && !NextChunk()) {
    return 0;
  }
  const size_t count = std::min(max_rows, rows_.size() - read_offset_);
  destination->Reset(schema_, count);
  for (size_t i = 0; i < count; ++i) {
    destination->Append(rows_[read_offset_ + i], RowPosition());
  }
  read_offset_ += count;
  return count;
}

void WindowExecutor::Dump(std::ostream& o, int indent) const {
  // Executors print no leading indent of their own: the parent emits the
  // child prefix (see LimitExecutor::Dump).
  o << "WindowExecutor(outputs=" << outputs_.size();
  if (spill_enabled_) {
    o << ", spill=partitioned, partitions=" << spills_.size()
      << ", resident-rows=" << peak_resident_rows_;
  }
  o << ")\n";
  if (child_ != nullptr) {
    o << Indent(static_cast<size_t>(indent) + 2);
    child_->Dump(o, indent + 2);
  }
}

void WindowExecutor::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
