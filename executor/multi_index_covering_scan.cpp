/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/multi_index_covering_scan.hpp"

#include <algorithm>
#include <cstddef>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "executor/bitmap_scan.hpp"
#include "executor/detail/scan_filter.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "index/index_scan_iterator.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/column_name.hpp"

namespace tinylamb {

MultiIndexCoveringScan::MultiIndexCoveringScan(
    Transaction& txn, const Table& table, std::vector<BitmapIndexRange> ranges,
    std::vector<TransactionManager::IndexReadGuard> guards, Expression where,
    std::vector<slot_t> output_slots, Schema output_schema)
    : txn_(txn),
      table_(table),
      guards_(std::move(guards)),
      where_(std::move(where)),
      where_conjuncts_(SplitConjuncts(where_)),
      output_slots_(std::move(output_slots)),
      output_schema_(std::move(output_schema)) {
  participants_.reserve(ranges.size());
  for (BitmapIndexRange& range : ranges) {
    Participant participant;
    participant.index = range.index;
    participant.begin_key = std::move(range.begin_key);
    participant.end_key = std::move(range.end_key);
    // The entry image TryReadIndexOnly fills is the index's own layout: key
    // columns first, then the INCLUDE payload.  Map every output column this
    // index stores to its position in that image.
    std::unordered_map<slot_t, size_t> image_index;
    size_t image_offset = 0;
    for (const slot_t key : range.index->sc_.key_) {
      image_index.emplace(key, image_offset++);
    }
    for (const slot_t include : range.index->sc_.include_) {
      image_index.emplace(include, image_offset++);
    }
    for (size_t out = 0; out < output_slots_.size(); ++out) {
      const auto found = image_index.find(output_slots_[out]);
      if (found != image_index.end()) {
        participant.fills.emplace_back(out, found->second);
      }
    }
    participants_.push_back(std::move(participant));
  }
}

void MultiIndexCoveringScan::FillFromImage(Row* values,
                                           const Participant& participant,
                                           const Row& image) const {
  for (const auto& [output, entry] : participant.fills) {
    if (entry < image.Count() && output < values->Count()) {
      (*values)[output] = image[entry];
    }
  }
}

void MultiIndexCoveringScan::Build() {
  built_ = true;
  // The planner orders the participants with the narrowest range first, so
  // the intersection table starts from the smallest side.
  for (size_t i = 0; i < participants_.size(); ++i) {
    const Participant& participant = participants_[i];
    IndexScanIterator iter(table_, *participant.index, txn_,
                           participant.begin_key, participant.end_key, true,
                           false, &guards_[i]);
    for (; iter.IsValid(); ++iter) {
      const RowPosition pos = iter.Position();
      if (!pos.IsValid()) {
        continue;
      }
      if (i == 0) {
        Candidate candidate;
        candidate.values.values_.resize(output_schema_.ColumnCount());
        Row image;
        if (iter.TryReadIndexOnly(&image)) {
          FillFromImage(&candidate.values, participant, image);
        } else {
          candidate.heap_needed = true;
        }
        // Duplicate entries of one index (repeated keys on a non-unique
        // index) collapse onto the same position.
        candidates_.emplace(pos, std::move(candidate));
        continue;
      }
      const auto found = candidates_.find(pos);
      if (found == candidates_.end()) {
        continue;
      }
      found->second.seen_generation = i;
      if (found->second.heap_needed) {
        continue;
      }
      Row image;
      if (iter.TryReadIndexOnly(&image)) {
        FillFromImage(&found->second.values, participant, image);
      } else {
        found->second.heap_needed = true;
      }
    }
    if (iter.GetStatus() != Status::kSuccess) {
      FailWith(iter.GetStatus());
      return;
    }
    if (i == 0) {
      continue;
    }
    // Drop every position this participant did not see: the surviving set is
    // the intersection of the participating ranges, which is exactly the rows
    // every participating conjunct admits (each range is a superset of the
    // rows satisfying its own conjunct).
    for (auto candidate = candidates_.begin();
         candidate != candidates_.end();) {
      if (candidate->second.seen_generation != i) {
        candidate = candidates_.erase(candidate);
      } else {
        ++candidate;
      }
    }
  }
  // Emit in row-position order: page-local locality, and the same order the
  // bitmap heap scan uses.
  positions_.reserve(candidates_.size());
  for (const auto& [pos, candidate] : candidates_) {
    positions_.push_back(pos);
  }
  std::sort(positions_.begin(), positions_.end(), RowPositionComparator{});
}

Row MultiIndexCoveringScan::AssembleFromHeap(const Row& heap_row) const {
  Row assembled;
  assembled.values_.reserve(output_slots_.size());
  for (const slot_t slot : output_slots_) {
    if (slot >= heap_row.Count()) {
      return Row();
    }
    assembled.values_.push_back(heap_row[slot]);
  }
  return assembled;
}

bool MultiIndexCoveringScan::Next(Row* dst, RowPosition* rp) {
  if (!built_) {
    Build();
  }
  if (GetStatus() != Status::kSuccess) {
    return false;
  }
  while (next_position_ < positions_.size()) {
    const RowPosition pos = positions_[next_position_++];
    const auto found = candidates_.find(pos);
    if (found == candidates_.end()) {
      continue;
    }
    Candidate candidate = std::move(found->second);
    candidates_.erase(found);
    Row assembled;
    if (candidate.heap_needed) {
      // A chain-bearing page means the index image is not provably this
      // snapshot's version, so resolve it through the heap.  A row deleted
      // out of the snapshot (or inserted after it) resolves to no version
      // and is skipped, exactly as the residual predicate would.
      StatusOr<Row> row_or = table_.Read(txn_, pos);
      if (row_or.GetStatus() == Status::kNotExists) {
        continue;
      }
      if (row_or.GetStatus() != Status::kSuccess) {
        return FailWith(row_or.GetStatus());
      }
      assembled = AssembleFromHeap(row_or.Value());
      if (!assembled.IsValid()) {
        continue;
      }
    } else {
      assembled = std::move(candidate.values);
    }
    if (where_) {
      // The covered columns carry every column the predicate touches, so the
      // recheck runs on the assembled row without any heap access.
      StatusOr<bool> pass = relational_detail::EvaluateConjunctsTolerant(
          where_conjuncts_, [&](const Expression& conjunct) {
            return conjunct->TryEvaluate(assembled, output_schema_);
          });
      if (!pass.HasValue()) {
        return FailWith(pass.GetStatus());
      }
      if (!pass.Value()) {
        continue;
      }
    }
    *dst = std::move(assembled);
    if (rp != nullptr) {
      *rp = pos;
    }
    return true;
  }
  return false;
}

void MultiIndexCoveringScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "MultiIndexCoveringScan: " << participants_.size() << " indexes";
  for (const Participant& participant : participants_) {
    o << "\n  Cover: " << participant.index->sc_.name_ << " on "
      << table_.GetSchema().Name();
  }
  if (where_) {
    o << "\n  Recheck: " << *where_;
  }
}

}  // namespace tinylamb
