/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_MULTI_INDEX_COVERING_SCAN_HPP
#define TINYLAMB_MULTI_INDEX_COVERING_SCAN_HPP

#include <cstddef>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/executor_base.hpp"
#include "index/index.hpp"
#include "page/row_position.hpp"
#include "plan/bitmap_scan_plan.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Table;
class Transaction;

// Multi-index covering scan (the executor half of
// MultiIndexCoveringScanPlan): intersects two or more index ranges by row
// position and assembles the output row from the index images, so a row whose
// page carries no version chains needs no heap fetch at all.  A position on a
// chain-bearing page falls back to the heap read (which resolves the snapshot
// version), and a position that is not in every participant's range is
// dropped -- the intersection is exactly the rows every participating
// conjunct admits, and the predicate recheck on the covered columns decides
// the rest.
//
// The caller must hold a read guard for every participant index (the plan's
// EmitExecutor checks the stale-key gate around the acquisitions) and must
// pass the participants ordered with the narrowest range first: the executor
// materializes the first participant's entries and filters them by the rest.
class MultiIndexCoveringScan : public ExecutorBase {
 public:
  MultiIndexCoveringScan(Transaction& txn, const Table& table,
                         std::vector<BitmapIndexRange> ranges,
                         std::vector<TransactionManager::IndexReadGuard> guards,
                         Expression where, std::vector<slot_t> output_slots,
                         Schema output_schema);
  MultiIndexCoveringScan(const MultiIndexCoveringScan&) = delete;
  MultiIndexCoveringScan(MultiIndexCoveringScan&&) = delete;
  MultiIndexCoveringScan& operator=(const MultiIndexCoveringScan&) = delete;
  MultiIndexCoveringScan& operator=(MultiIndexCoveringScan&&) = delete;
  ~MultiIndexCoveringScan() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  struct Participant {
    const Index* index{nullptr};
    std::vector<Value> begin_key;
    std::vector<Value> end_key;
    // (output column, entry image column) pairs this participant fills.  The
    // entry image is the index's own key-then-INCLUDE layout, so a covered
    // output column maps to a fixed position in every entry it appears in.
    std::vector<std::pair<size_t, size_t>> fills;
  };
  struct Candidate {
    Row values;
    // The participant pass that last saw this position; a pass that does not
    // see it drops the position from the intersection.
    size_t seen_generation{0};
    // Some participant could not serve the position index-only, so the row
    // has to come from the heap (snapshot-resolved).
    bool heap_needed{false};
  };

  void Build();
  void FillFromImage(Row* values, const Participant& participant,
                     const Row& image) const;
  [[nodiscard]] Row AssembleFromHeap(const Row& heap_row) const;

  Transaction& txn_;
  const Table& table_;
  std::vector<Participant> participants_;
  std::vector<TransactionManager::IndexReadGuard> guards_;
  Expression where_;
  std::vector<Expression> where_conjuncts_;
  std::vector<slot_t> output_slots_;
  Schema output_schema_;
  std::unordered_map<RowPosition, Candidate> candidates_;
  std::vector<RowPosition> positions_;
  size_t next_position_{0};
  bool built_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_MULTI_INDEX_COVERING_SCAN_HPP
