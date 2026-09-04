/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_RECURSIVE_CTE_EXECUTOR_HPP
#define TINYLAMB_RECURSIVE_CTE_EXECUTOR_HPP

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "page/row_position.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class TransactionContext;

class RecursiveCteExecutor final : public ExecutorBase {
 public:
  RecursiveCteExecutor(TransactionContext& context, std::string cte_name,
                       std::shared_ptr<const SelectStatement> body,
                       std::optional<RecursiveDepthSpec> depth_spec,
                       Schema output_schema);

  RecursiveCteExecutor(const RecursiveCteExecutor&) = delete;
  RecursiveCteExecutor(RecursiveCteExecutor&&) = delete;
  RecursiveCteExecutor& operator=(const RecursiveCteExecutor&) = delete;
  RecursiveCteExecutor& operator=(RecursiveCteExecutor&&) = delete;
  ~RecursiveCteExecutor() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  void Initialize();

  TransactionContext& context_;
  std::string cte_name_;
  std::shared_ptr<const SelectStatement> body_;
  std::optional<RecursiveDepthSpec> depth_spec_;
  Schema output_schema_;

  bool initialized_{false};
  std::vector<Row> rows_;
  size_t row_idx_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_RECURSIVE_CTE_EXECUTOR_HPP
