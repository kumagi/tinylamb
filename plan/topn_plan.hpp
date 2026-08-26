/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_TOPN_PLAN_HPP
#define TINYLAMB_PLAN_TOPN_PLAN_HPP

#include <optional>
#include <utility>
#include <vector>

#include "plan/plan.hpp"

namespace tinylamb {

struct TopNKey {
  Expression expression;
  bool ascending{true};
  std::optional<bool> nulls_first;
};

class TopNPlan final : public PlanBase {
 public:
  TopNPlan(Plan child, std::vector<TopNKey> keys, size_t limit, size_t offset,
           bool with_ties = false)
      : child_(std::move(child)),
        keys_(std::move(keys)),
        limit_(limit),
        offset_(offset),
        with_ties_(with_ties) {}

  Executor EmitExecutor(TransactionContext& context) const override;
  [[nodiscard]] const Table* ScanSource() const override {
    return child_->ScanSource();
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return child_->GetStats();
  }
  [[nodiscard]] const Schema& GetSchema() const override {
    return child_->GetSchema();
  }
  [[nodiscard]] size_t AccessRowCount() const override {
    return child_->AccessRowCount();
  }
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool EnforcesLimit(size_t limit, size_t offset) const override {
    return !with_ties_ && limit == limit_ && offset == offset_;
  }
  [[nodiscard]] const Plan& Child() const { return child_; }
  [[nodiscard]] const std::vector<TopNKey>& Keys() const { return keys_; }
  [[nodiscard]] size_t Limit() const { return limit_; }
  [[nodiscard]] size_t Offset() const { return offset_; }
  [[nodiscard]] bool WithTies() const { return with_ties_; }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  std::vector<TopNKey> keys_;
  size_t limit_{0};
  size_t offset_{0};
  bool with_ties_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_TOPN_PLAN_HPP
