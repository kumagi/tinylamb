#ifndef TINYLAMB_PLAN_SAMPLE_PLAN_HPP
#define TINYLAMB_PLAN_SAMPLE_PLAN_HPP

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "expression/table_sample_spec.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class SamplePlan final : public PlanBase {
 public:
  SamplePlan(Plan child, TableSampleSpec spec);
  SamplePlan(const SamplePlan&) = delete;
  SamplePlan(SamplePlan&&) = delete;
  SamplePlan& operator=(const SamplePlan&) = delete;
  SamplePlan& operator=(SamplePlan&&) = delete;
  ~SamplePlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] bool ContainsParameterSlots() const override;
  bool InstallParameterSlots(const std::vector<Value>& parameters,
                             size_t* cursor,
                             std::vector<Expression>* slots) override;

  [[nodiscard]] const Table* ScanSource() const override {
    return child_->ScanSource();
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return child_->GetStats();
  }
  [[nodiscard]] const Schema& GetSchema() const override {
    return output_schema_;
  }
  [[nodiscard]] size_t AccessRowCount() const override {
    return child_->AccessRowCount();
  }
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override {
    if (spec_.method == TableSampleMethod::kReservoir ||
        spec_.method == TableSampleMethod::kSystem) {
      return false;
    }
    return child_->IsOrderedBy(expressions, ascending);
  }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override {
    if (spec_.method == TableSampleMethod::kReservoir ||
        spec_.method == TableSampleMethod::kSystem) {
      return false;
    }
    return child_->IsOrderedBy(expressions, ascending, nulls_first);
  }
  [[nodiscard]] bool EnforcesDistinct() const override {
    return child_->EnforcesDistinct();
  }
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  static Schema BuildSchema(const Schema& child, const TableSampleSpec& spec);

  Plan child_;
  TableSampleSpec spec_;
  Schema output_schema_;
};

}  // namespace tinylamb

#endif
