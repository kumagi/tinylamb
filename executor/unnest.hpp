/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_UNNEST_HPP
#define TINYLAMB_EXECUTOR_UNNEST_HPP

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TransactionContext;

class UnnestExecutor final : public ExecutorBase {
 public:
  UnnestExecutor(TransactionContext& ctx, Schema child_schema, Executor child,
                 Expression unnest_expr, std::string alias,
                 std::string offset_alias);

  UnnestExecutor(const UnnestExecutor&) = delete;
  UnnestExecutor(UnnestExecutor&&) = delete;
  UnnestExecutor& operator=(const UnnestExecutor&) = delete;
  UnnestExecutor& operator=(UnnestExecutor&&) = delete;
  ~UnnestExecutor() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  TransactionContext& ctx_;
  Schema child_schema_;
  Executor child_;
  Expression unnest_expr_;
  std::string alias_;
  std::string offset_alias_;

  Row current_child_row_;
  std::vector<Row> current_unnested_rows_;
  size_t current_row_index_{0};
  bool child_exhausted_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_UNNEST_HPP
