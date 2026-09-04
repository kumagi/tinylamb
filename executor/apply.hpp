/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_APPLY_EXECUTOR_HPP
#define TINYLAMB_APPLY_EXECUTOR_HPP

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/join_kind.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class TransactionContext;

class ApplyExecutor final : public ExecutorBase {
 public:
  ApplyExecutor(TransactionContext& context, Schema outer_schema,
                Executor outer_exec,
                std::shared_ptr<const SelectStatement> inner_query,
                std::string inner_alias, Executor inner_exec,
                Expression predicate, JoinKind kind, Schema output_schema);

  ApplyExecutor(const ApplyExecutor&) = delete;
  ApplyExecutor(ApplyExecutor&&) = delete;
  ApplyExecutor& operator=(const ApplyExecutor&) = delete;
  ApplyExecutor& operator=(ApplyExecutor&&) = delete;
  ~ApplyExecutor() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  TransactionContext& context_;
  Schema outer_schema_;
  Executor outer_exec_;
  std::shared_ptr<const SelectStatement> inner_query_;
  std::string inner_alias_;
  Executor inner_exec_;
  Expression predicate_;
  JoinKind kind_;
  Schema output_schema_;

  Schema inner_schema_;
  Schema combined_schema_;

  bool has_outer_row_{false};
  Row current_outer_row_;
  RowPosition current_outer_rp_;

  std::vector<Row> inner_rows_;
  size_t inner_idx_{0};
  bool matched_any_{false};

  std::vector<Row> static_inner_rows_;
  bool static_inner_cached_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_APPLY_EXECUTOR_HPP
