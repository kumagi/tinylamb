/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GROUPING_SETS_EXECUTOR_HPP
#define TINYLAMB_GROUPING_SETS_EXECUTOR_HPP

#include <cstddef>
#include <memory>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

class GroupingSetsExecutor : public ExecutorBase {
 public:
  GroupingSetsExecutor(Executor child, Schema input_schema,
                       std::vector<NamedExpression> all_group_keys,
                       std::vector<std::vector<size_t>> grouping_sets,
                       std::vector<NamedExpression> aggregates);
  GroupingSetsExecutor(const GroupingSetsExecutor&) = delete;
  GroupingSetsExecutor(GroupingSetsExecutor&&) = delete;
  GroupingSetsExecutor& operator=(const GroupingSetsExecutor&) = delete;
  GroupingSetsExecutor& operator=(GroupingSetsExecutor&&) = delete;
  ~GroupingSetsExecutor() override = default;

  static GroupingSetsExecutor Rollup(
      Executor child, Schema input_schema,
      std::vector<NamedExpression> all_group_keys,
      std::vector<NamedExpression> aggregates);

  static GroupingSetsExecutor Cube(Executor child, Schema input_schema,
                                   std::vector<NamedExpression> all_group_keys,
                                   std::vector<NamedExpression> aggregates);

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] const Schema& OutputSchema() const { return output_schema_; }

 private:
  void Materialize();

  Executor child_;
  Schema input_schema_;
  std::vector<NamedExpression> all_group_keys_;
  std::vector<std::vector<size_t>> grouping_sets_;
  std::vector<NamedExpression> aggregates_;
  Schema output_schema_;

  bool materialized_{false};
  std::vector<Row> output_rows_;
  size_t cursor_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_GROUPING_SETS_EXECUTOR_HPP
