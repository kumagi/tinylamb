/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_EXECUTOR_WINDOW_HPP
#define TINYLAMB_EXECUTOR_WINDOW_HPP

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TransactionContext;

// Evaluates window-function calls computed by the shared planning-time
// extraction (one `$winN` output column per call) over a row-preserving
// child. Each call runs through the canonical window evaluator
// (executor/detail/window_eval), so engine and planner agree on ranking,
// framing, and NULL placement by construction. Input order is preserved;
// the whole input is buffered (window functions need partition context).
class WindowExecutor : public ExecutorBase {
 public:
  WindowExecutor(const WindowExecutor&) = delete;
  WindowExecutor& operator=(const WindowExecutor&) = delete;
  WindowExecutor(WindowExecutor&&) = delete;
  WindowExecutor& operator=(WindowExecutor&&) = delete;
  WindowExecutor(TransactionContext& context, Executor child, Schema schema,
                 std::vector<NamedExpression> outputs);
  ~WindowExecutor() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

 private:
  bool EnsureComputed();

  TransactionContext& context_;
  Executor child_;
  Schema schema_;
  std::vector<NamedExpression> outputs_;
  std::vector<Row> rows_;
  size_t read_offset_{0};
  bool computed_{false};
  // Number of outputs already appended to rows_. A mid-loop failure leaves
  // these in place while computed_ stays false; the next EnsureComputed
  // resumes from here instead of appending duplicate columns.
  size_t appended_columns_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_WINDOW_HPP
