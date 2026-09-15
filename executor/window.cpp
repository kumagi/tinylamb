/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/window.hpp"

#include <cassert>
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
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

WindowExecutor::WindowExecutor(TransactionContext& context, Executor child,
                               Schema schema,
                               std::vector<NamedExpression> outputs)
    : context_(context),
      child_(std::move(child)),
      schema_(std::move(schema)),
      outputs_(std::move(outputs)) {}

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
    Row row;
    RowPosition rp;
    while (child_->Next(&row, &rp)) {
      input.push_back(std::move(row));
    }
    if (child_->GetStatus() != Status::kSuccess) {
      return FailWithChildOf(*child_);
    }
    rows_.reserve(input.size());
    for (Row& input_row : input) {
      rows_.push_back(std::move(input_row));
    }
  }
  // The output schema appends one column per call; strip those suffix
  // columns to recover the schema the calls resolve against.
  std::vector<Column> input_columns;
  const size_t input_width = schema_.ColumnCount() >= outputs_.size()
                                 ? schema_.ColumnCount() - outputs_.size()
                                 : 0;
  input_columns.reserve(input_width);
  for (size_t i = 0; i < input_width; ++i) {
    input_columns.push_back(schema_.GetColumn(i));
  }
  Schema input_schema(schema_.Name(), std::move(input_columns));
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
    const relational_detail::CteMap empty_ctes;
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
  // Only mark computed after every call succeeded: a later Next() must not
  // replay the partially extended rows_ (they would be missing output
  // columns).
  computed_ = true;
  return true;
}

bool WindowExecutor::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  if (!EnsureComputed()) {
    return false;
  }
  if (read_offset_ >= rows_.size()) {
    return false;
  }
  *dst = rows_[read_offset_];
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  ++read_offset_;
  return true;
}

size_t WindowExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  if (!EnsureComputed()) {
    return 0;
  }
  if (read_offset_ >= rows_.size()) {
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
  o << "WindowExecutor(outputs=" << outputs_.size() << ")\n";
  if (child_ != nullptr) {
    o << Indent(static_cast<size_t>(indent) + 2);
    child_->Dump(o, indent + 2);
  }
}

void WindowExecutor::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
