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

#include "expression/expression.hpp"

namespace tinylamb {
class Table;
class TransactionContext;
class TableStatistics;
class Index;
// The executor handle is opaque at the plan layer (V4): plans carry logical
// information, and the relational factory in the executor layer implements
// EmitExecutor for every concrete plan node.
class ExecutorBase;
using Executor = std::shared_ptr<ExecutorBase>;

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
  [[nodiscard]] virtual bool IsOrderedBy(
      const std::vector<Expression>& /*expressions*/,
      const std::vector<bool>& /*ascending*/) const {
    return false;
  }
  // True when the plan already caps its output to `limit_count` rows after
  // skipping `limit_offset`, so callers must not apply the same LIMIT/OFFSET
  // again (D6: single enforcement point).
  [[nodiscard]] virtual bool EnforcesLimit(
      size_t /*limit_count*/, size_t /*limit_offset*/) const {
    return false;
  }

  virtual void Dump(std::ostream& o, int indent) const = 0;
  [[nodiscard]] virtual std::string ToString() const = 0;
  friend std::ostream& operator<<(std::ostream& o, const PlanBase& p);
};

using Plan = std::shared_ptr<PlanBase>;

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
