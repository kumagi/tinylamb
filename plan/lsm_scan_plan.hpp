/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#ifndef TINYLAMB_PLAN_LSM_SCAN_PLAN_HPP
#define TINYLAMB_PLAN_LSM_SCAN_PLAN_HPP

#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class LSMTree;

// A scan over an LSM/WiscKey tree (index/lsm_tree.hpp): rows are the live
// (key, value) pairs of the tree's snapshot view -- newest generation wins,
// tombstones are skipped.  The tree is caller-owned and must outlive the
// plan and its executors.
//
// Reach: SQL does not produce the kLsmScan logical node; plan-level callers
// and tests build the memo directly and the `lsm_scan` implementation rule
// turns it into this plan (no SQL surface yet).
//
// Output schema: (key VARCHAR, value VARCHAR), raw bytes as stored.  Keys
// stream in byte order; the plan claims ascending order on `key` only.  No
// row carries a NULL key, so a NULL placement request is always satisfied.
class LsmScanPlan final : public PlanBase {
 public:
  LsmScanPlan(const LSMTree& tree, size_t estimated_rows);

  Executor EmitExecutor(TransactionContext& context) const override;
  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }
  [[nodiscard]] const Schema& GetSchema() const override { return schema_; }
  [[nodiscard]] size_t AccessRowCount() const override {
    return estimated_rows_;
  }
  [[nodiscard]] size_t EmitRowCount() const override { return estimated_rows_; }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override;
  [[nodiscard]] const LSMTree& GetTree() const { return tree_; }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  const LSMTree& tree_;
  Schema schema_;
  TableStatistics stats_;
  size_t estimated_rows_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_LSM_SCAN_PLAN_HPP
