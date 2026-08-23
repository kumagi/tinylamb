/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_LIMIT_PLAN_HPP
#define TINYLAMB_LIMIT_PLAN_HPP

#include <cstddef>

#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

// Physical LIMIT/OFFSET: keeps LIMIT ownership inside the optimizer while the
// query engine's LimitExecutor wrapper remains as the safety net (D6). Above
// an order-delivering scan it degenerates into a Top-K pipeline because the
// executor stack is lazy.
class LimitPlan final : public PlanBase {
 public:
  LimitPlan(Plan src, size_t limit_count, size_t limit_offset)
      : src_(std::move(src)),
        limit_count_(limit_count),
        limit_offset_(limit_offset) {}
  LimitPlan(const LimitPlan&) = delete;
  LimitPlan(LimitPlan&&) = delete;
  LimitPlan& operator=(const LimitPlan&) = delete;
  LimitPlan& operator=(LimitPlan&&) = delete;
  ~LimitPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override {
    return src_->ScanSource();
  }
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return src_->GetStats();
  }
  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] bool IsOrderedBy(const std::vector<Expression>& expressions,
                                 const std::vector<bool>& ascending) const override {
    return src_->IsOrderedBy(expressions, ascending);
  }
  [[nodiscard]] bool EnforcesLimit(size_t limit_count,
                                   size_t limit_offset) const override {
    return limit_count == limit_count_ && limit_offset == limit_offset_;
  }
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan src_;
  size_t limit_count_;
  size_t limit_offset_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LIMIT_PLAN_HPP
