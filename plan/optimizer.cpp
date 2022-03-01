//
// Created by kumagi on 2022/03/02.
//

#include "optimizer.hpp"

#include "database/transaction_context.hpp"
#include "plan.hpp"

namespace tinylamb {

std::unique_ptr<ExecutorBase> Optimizer::Optimize(TransactionContext& ctx) {
  if (query_.tables_.empty()) throw std::runtime_error("No table specified");

  Plan result = Plan::FullScan(query_.tables_[0]);
  for (size_t i = 1; i < query_.tables_.size(); ++i) {
    result = Plan::Product(result, Plan::FullScan(query_.tables_[i]));
  }
  result = Plan::Selection(result, query_.expression_);

  Schema final_schema = result.GetSchema(ctx);
  std::unordered_map<std::string, size_t> offset_map;
  offset_map.reserve(final_schema.ColumnCount());
  for (size_t i = 0; i < final_schema.ColumnCount(); ++i) {
    offset_map.emplace(final_schema.GetColumn(i).Name(), i);
  }
  std::vector<size_t> columns;
  columns.reserve(query_.columns_.size());
  for (const auto& col : query_.columns_) {
    const auto it = offset_map.find(col);
    if (it == offset_map.end()) {
      LOG(ERROR) << "specified Column[" << col << "] does not exists";
      return {};
    }
    columns.push_back(offset_map[col]);
  }
  result = Plan::Projection(result, columns);
  return result.EmitExecutor(ctx);
}

}  // namespace tinylamb