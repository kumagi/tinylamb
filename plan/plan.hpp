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

#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "type/collation.hpp"

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

struct PlanExplainInfo {
  std::string rule_name{};
  std::string physical_operator{};
  std::string required_properties{};
  std::string provided_properties{};
  std::string applied_rules{};
};

class PlanBase {
 public:
  PlanBase() = default;
  PlanBase(const PlanBase&) = delete;
  PlanBase& operator=(const PlanBase&) = delete;
  PlanBase(PlanBase&&) = delete;
  PlanBase& operator=(PlanBase&&) = delete;
  virtual ~PlanBase() = default;

  virtual Executor EmitExecutor(TransactionContext& txn) const = 0;

  // True when this node or a descendant still holds a ParameterSlot, so a
  // compiled-plan replay can rebind literals. The default is false: a wrapper
  // that forgets to forward simply disables parametric reuse, which recompiles
  // instead of executing sniffed constants against a different literal.
  [[nodiscard]] virtual bool ContainsParameterSlots() const { return false; }

  // Collects ParameterSlots already stamped in SQL-text order. Parent
  // expressions are visited before children. The default has no literals.
  virtual bool InstallParameterSlots(const std::vector<Value>& /*parameters*/,
                                     size_t* /*cursor*/,
                                     std::vector<Expression>* /*slots*/) {
    return true;
  }

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
  [[nodiscard]] virtual bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& /*nulls_first*/) const {
    static_cast<void>(expressions);
    static_cast<void>(ascending);
    return false;
  }
  // The collation this plan's delivered ordering was produced under
  // (kNone = the engine default, i.e. binary byte order). ORDER BY
  // comparators are collation sensitive — under a case-insensitive
  // collation 'a' sorts before 'B', under binary 'B' sorts before 'a' — so
  // an ordering is only good for a consumer that asked for the same
  // collation. Comparison-based operators (sort, top-n) override this;
  // index and scan orders are storage byte orders and keep the default.
  [[nodiscard]] virtual Collation OrderCollation() const {
    return Collation::kNone;
  }
  // True when the ordering this plan reports was MEASURED from the stored
  // layout (a provably clustered full scan) instead of produced by a sorted
  // structure such as an index or a sort operator.
  //
  // Costing has to tell the two apart. A structural order is free for the scan
  // that yields it, so the optimizer may prefer such a plan over a cheaper
  // unordered one and may read a LIMIT above it as an early stop. A measured
  // order is not: the scan already walked a whole row chain to obtain it, and
  // the LIMIT stays above the scan, so such a plan has to win on cost alone.
  // Transparent wrappers (projection / selection / limit) forward this to their
  // child; every other plan keeps the default.
  [[nodiscard]] virtual bool OrderIsMeasured() const { return false; }
  // True when the plan already caps its output to `limit_count` rows after
  // skipping `limit_offset`, so callers must not apply the same LIMIT/OFFSET
  // again (D6: single enforcement point).
  [[nodiscard]] virtual bool EnforcesLimit(size_t /*limit_count*/,
                                           size_t /*limit_offset*/) const {
    return false;
  }
  // True when the plan guarantees that all output rows are distinct, so callers
  // must not apply a redundant DistinctExecutor wrapper (D6: single enforcement
  // point).
  [[nodiscard]] virtual bool EnforcesDistinct() const { return false; }

  void SetExplainInfo(PlanExplainInfo info) { explain_info_ = std::move(info); }
  [[nodiscard]] const PlanExplainInfo& ExplainInfo() const {
    return explain_info_;
  }

  virtual void Dump(std::ostream& o, int indent) const = 0;
  virtual void Explain(std::ostream& o, int indent) const;
  [[nodiscard]] virtual std::string ToString() const = 0;
  friend std::ostream& operator<<(std::ostream& o, const PlanBase& p);

 private:
  PlanExplainInfo explain_info_{};
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
