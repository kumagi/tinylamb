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

#ifndef TINYLAMB_PLAN_HPP
#define TINYLAMB_PLAN_HPP

#include <memory>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Table;
class TransactionContext;
class TableStatistics;
class Index;

class PlanBase {
 public:
  PlanBase() = default;
  PlanBase(const PlanBase&) = delete;
  PlanBase& operator=(const PlanBase&) = delete;
  PlanBase(PlanBase&&) = delete;
  PlanBase& operator=(PlanBase&&) = delete;
  virtual ~PlanBase() = default;

  virtual Executor EmitExecutor(TransactionContext& txn) const = 0;

  [[nodiscard]] virtual const Table* ScanSource() const = 0;
  [[nodiscard]] virtual const TableStatistics& GetStats() const = 0;
  [[nodiscard]] virtual const Schema& GetSchema() const = 0;
  [[nodiscard]] virtual size_t AccessRowCount() const = 0;
  [[nodiscard]] virtual size_t EmitRowCount() const = 0;

  virtual void Dump(std::ostream& o, int indent) const = 0;
  [[nodiscard]] virtual std::string ToString() const = 0;
  friend std::ostream& operator<<(std::ostream& o, const PlanBase& p);
};

typedef std::shared_ptr<PlanBase> Plan;

inline std::ostream& operator<<(std::ostream& o, const Plan& p) {
  if (p) {
    p->Dump(o, 0);
  } else {
    o << "(null plan)";
  }
  return o;
}
}  // namespace tinylamb
#endif  // TINYLAMB_PLAN_HPP
