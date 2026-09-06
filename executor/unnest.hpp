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
  // `output_schema` (optional) is the plan-level schema for the unnest
  // section; per-row array shapes can disagree with it (a NULL element in one
  // array decodes through the scalar path while another row flattens struct
  // members), so emitted rows are re-projected onto the plan layout by name.
  UnnestExecutor(TransactionContext& ctx, Schema child_schema, Executor child,
                 Expression unnest_expr, std::string alias,
                 std::string offset_alias, bool left_outer = false,
                 Schema output_schema = Schema("", {}));

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
  // LEFT / FULL OUTER UNNEST: when the array is empty (or NULL) the driving
  // row still emits once with NULL-extended unnest columns instead of
  // disappearing.  INNER (the default) drops the row entirely.
  bool left_outer_{false};
  Schema output_schema_;

  Row current_child_row_;
  std::vector<Row> current_unnested_rows_;
  size_t current_row_index_{0};
  bool child_exhausted_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_UNNEST_HPP
