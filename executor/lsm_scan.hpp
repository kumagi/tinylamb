/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#ifndef TINYLAMB_LSM_SCAN_HPP
#define TINYLAMB_LSM_SCAN_HPP

#include <optional>

#include "executor/executor_base.hpp"
#include "index/lsm_detail/lsm_view.hpp"

namespace tinylamb {
class LSMTree;

// Streams an LSM/WiscKey tree's live (key, value) pairs as rows
// (key VARCHAR, value VARCHAR).  The snapshot view is taken at construction
// (LSMTree::GetView): the scan observes one consistent set of runs; entries
// written after the scan started are not seen.  Keys stream in byte order;
// tombstones are skipped and the newest generation wins.
//
// No row position: an LSM entry is not a table row (rp is left untouched and
// DML never consumes this executor).
class LsmScan final : public ExecutorBase {
 public:
  explicit LsmScan(const LSMTree& tree);

  bool Next(Row* destination, RowPosition* position) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  // Order matters: iter_ holds a back pointer to view_, so the view must be
  // constructed first and never move afterwards (both are members of a
  // heap-allocated executor, so their addresses are stable).
  LSMView view_;
  std::optional<LSMView::Iterator> iter_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LSM_SCAN_HPP
