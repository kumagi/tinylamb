/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#include "plan/projection_plan.hpp"

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/executor_base.hpp"
#include "executor/projection.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "plan.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"

namespace tinylamb {
ProjectionPlan::ProjectionPlan(Plan src,
                               std::vector<NamedExpression> project_columns)
    : src_(std::move(src)),
      columns_(std::move(project_columns)),
      output_schema_(CalcSchema()),
      stats_(src_->GetStats()) {}

ProjectionPlan::ProjectionPlan(Plan src,
                               const std::vector<ColumnName>& project_columns)
    : src_(std::move(std::move(src))),
      output_schema_(CalcSchema()),
      stats_(src->GetStats()) {
  columns_.reserve(project_columns.size());
  for (const auto& col : project_columns) {
    columns_.emplace_back(col);
  }
}

Schema ProjectionPlan::CalcSchema() const {
  Schema original_schema = src_->GetSchema();

  std::vector<Column> cols;
  cols.reserve(columns_.size());
  for (size_t i = 0; i < columns_.size(); ++i) {
    if (!columns_[i].name.empty()) {
      cols.emplace_back(columns_[i].name);
      continue;
    }
    if (columns_[i].expression->Type() == TypeTag::kColumnValue) {
      auto* cv = dynamic_cast<ColumnValue*>(columns_[i].expression.get());
      cols.emplace_back(ColumnName(cv->col_name_));
      continue;
    }
    cols.emplace_back(ColumnName("$col" + std::to_string(i)));
  }
  return {"", cols};
}

Executor ProjectionPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<Projection>(columns_, src_->GetSchema(),
                                      src_->EmitExecutor(ctx));
}

const Schema& ProjectionPlan::GetSchema() const { return output_schema_; }

size_t ProjectionPlan::AccessRowCount() const { return src_->AccessRowCount(); }

size_t ProjectionPlan::EmitRowCount() const { return src_->EmitRowCount(); }

void ProjectionPlan::Dump(std::ostream& o, int indent) const {
  o << "Project: {";
  for (size_t i = 0; i < columns_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << columns_[i];
  }
  o << "} (estimated cost: " << AccessRowCount() << ")\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}
}  // namespace tinylamb
