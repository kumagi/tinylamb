/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_RELATION_RENAME_PLAN_HPP
#define TINYLAMB_PLAN_RELATION_RENAME_PLAN_HPP

#include <string>

#include "plan/plan.hpp"

namespace tinylamb {

// Presents a child plan's output under a different relation identity
// (Phase 8 aliases/self-joins): rows stream through unchanged and positions
// are preserved, while every column is re-qualified from the physical table
// name to the relation key. Ordering requests are translated back down so
// index-provided order stays visible above the rename (Top-K, sort
// elimination).
class RelationRenamePlan final : public PlanBase {
 public:
  RelationRenamePlan(Plan src, std::string relation, std::string physical);
  RelationRenamePlan(const RelationRenamePlan&) = delete;
  RelationRenamePlan(RelationRenamePlan&&) = delete;
  RelationRenamePlan& operator=(const RelationRenamePlan&) = delete;
  RelationRenamePlan& operator=(RelationRenamePlan&&) = delete;
  ~RelationRenamePlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override {
    return src_->ScanSource();
  }
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return src_->GetStats();
  }
  [[nodiscard]] size_t AccessRowCount() const override {
    return src_->AccessRowCount();
  }
  [[nodiscard]] size_t EmitRowCount() const override {
    return src_->EmitRowCount();
  }
  [[nodiscard]] bool IsOrderedBy(const std::vector<Expression>& expressions,
                                 const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool EnforcesLimit(size_t limit_count,
                                   size_t limit_offset) const override {
    return src_->EnforcesLimit(limit_count, limit_offset);
  }
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan src_;
  std::string relation_;
  std::string physical_;
  Schema renamed_schema_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_RELATION_RENAME_PLAN_HPP
