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

#include "hash_join.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/executor_base.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/data_chunk.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "page/row_position.hpp"

namespace tinylamb {
namespace {

constexpr size_t kReactiveSpillPartitions = 16;

size_t ReactivePartitionOf(const std::string& key) {
  return std::hash<std::string>{}(key) % kReactiveSpillPartitions;
}

// Encodes the join key columns; returns false when any component is NULL.
// SQL semantics: NULL never compares equal to anything, so such rows cannot
// match and must be skipped rather than memcomparable-encoded (encoding a
// kNull Value throws).
bool EncodeJoinKey(const Row& row, const std::vector<slot_t>& cols,
                   std::string* out) {
  const Row keys = row.Extract(cols);
  for (const Value& value : keys.values_) {
    if (value.IsNull()) {
      return false;
    }
  }
  *out = keys.EncodeMemcomparableFormat();
  return true;
}

void JoinPartition(
    const std::vector<slot_t>& left_cols, const std::vector<slot_t>& right_cols,
    const std::vector<std::pair<Row, RowPosition>>& left_rows,
    const std::vector<Row>& right_rows,
    std::vector<std::pair<Row, RowPosition>>* output) {
  std::unordered_multimap<std::string, const Row*> buckets;
  buckets.reserve(right_rows.size());
  for (const Row& right_row : right_rows) {
    std::string key;
    if (!EncodeJoinKey(right_row, right_cols, &key)) { continue;
}
    buckets.emplace(std::move(key), &right_row);
  }
  for (const auto& left : left_rows) {
    std::string key;
    if (!EncodeJoinKey(left.first, left_cols, &key)) { continue;
}
    const auto [begin, end] = buckets.equal_range(key);
    for (auto match = begin; match != end; ++match) {
      output->emplace_back(left.first + *match->second, left.second);
    }
  }
}

void ChargeOutput(std::vector<std::pair<Row, RowPosition>>* output,
                  QueryMemoryCharge* output_charge) {
  size_t output_bytes = 0;
  for (const auto& out : *output) {
    output_bytes += EstimateRowBytes(out.first);
  }
  *output_charge = QueryMemoryCharge(output_bytes);
}

}  // namespace

HashJoin::HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
                   std::vector<slot_t> right_cols, size_t worker_count)
    : HashJoin(std::move(left), std::move(left_cols), std::move(right),
               std::move(right_cols), HashJoinMode::kInMemory, worker_count) {}

HashJoin::HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
                   std::vector<slot_t> right_cols, HashJoinMode mode,
                   size_t worker_count)
    : left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(std::move(right)),
      right_cols_(std::move(right_cols)),
      mode_(mode),
      worker_count_(std::max<size_t>(1, worker_count)) {}

bool HashJoin::EmitNextMatch(Row* dst, RowPosition* rp) {
  while (true) {
    if (match_iter_ != match_end_) {
      *dst = current_left_ + *match_iter_->second;
      if (rp != nullptr) {
        *rp = current_left_pos_;
      }
      ++match_iter_;
      return true;
    }
    Row row;
    RowPosition position;
    bool have_probe_row = false;
    std::string key;
    while (left_->Next(&row, &position)) {
      if (!EncodeJoinKey(row, left_cols_, &key)) { continue;  // NULL key never matches
}
      current_left_ = std::move(row);
      current_left_pos_ = position;
      have_probe_row = true;
      break;
    }
    if (!have_probe_row) {
      left_exhausted_ = true;
      return false;
    }
    const auto [begin, end] = right_buckets_.equal_range(key);
    match_iter_ = begin;
    match_end_ = end;
  }
}

void HashJoin::MaterializeOrThrow() {
  if (materialize_failed_) {
    // A previous attempt threw midway; children are partially consumed and
    // retrying would corrupt results.
    throw std::runtime_error("hash join materialization previously failed");
  }
  try {
    Materialize();
  } catch (...) {
    materialize_failed_ = true;
    throw;
  }
}

bool HashJoin::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) {
    MaterializeOrThrow();
  }
  if (pipelined_) {
    return EmitNextMatch(dst, rp);
  }
  if (output_offset_ == output_.size()) {
    return false;
  }
  *dst = output_[output_offset_].first;
  if (rp != nullptr) {
    *rp = output_[output_offset_].second;
  }
  ++output_offset_;
  return true;
}

size_t HashJoin::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (!materialized_) {
    MaterializeOrThrow();
  }
  if (pipelined_) {
    Row row;
    RowPosition position;
    while (destination->Size() < max_rows && EmitNextMatch(&row, &position)) {
      destination->Append(row, position);
    }
    return destination->Size();
  }
  while (output_offset_ < output_.size() &&
         destination->Size() < max_rows) {
    destination->Append(output_[output_offset_].first,
                        output_[output_offset_].second);
    ++output_offset_;
  }
  return destination->Size();
}

void HashJoin::Materialize() {
  // Never resume from a partially filled output of a failed attempt; a retry
  // must start from an empty output or it would emit duplicate rows.
  output_.clear();
  output_offset_ = 0;
  if (mode_ == HashJoinMode::kHybrid) {
    MaterializeHybrid();
  } else {
    MaterializeInMemory();
  }
}

void HashJoin::MaterializeInMemory() {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  QueryMemoryCharge charge;

  std::vector<Row> right_rows;
  std::vector<SpillFile> right_spill(kReactiveSpillPartitions);
  bool spilled = false;
  std::string key;

  Row row;
  while (right_->Next(&row, nullptr)) {
    const size_t bytes = EstimateRowBytes(row);
    if (!spilled && !budget.CanReserve(bytes)) {
      spilled = true;
      for (const Row& existing : right_rows) {
        if (!EncodeJoinKey(existing, right_cols_, &key)) { continue;
}
        right_spill[ReactivePartitionOf(key)].Append(existing);
      }
      charge.ReleaseAll();
      right_rows.clear();
      right_rows.shrink_to_fit();
    }
    if (spilled) {
      if (!EncodeJoinKey(row, right_cols_, &key)) { continue;
}
      right_spill[ReactivePartitionOf(key)].Append(row);
    } else {
      charge.Add(bytes);
      right_rows.push_back(std::move(row));
    }
  }

  if (!spilled) {
    // Build side fit in memory: keep it resident and probe pipelined.
    right_storage_ = std::move(right_rows);
    right_buckets_.reserve(right_storage_.size());
    for (const Row& stored : right_storage_) {
      if (!EncodeJoinKey(stored, right_cols_, &key)) { continue;
}
      right_buckets_.emplace(key, &stored);
    }
    pipelined_ = true;
    materialized_ = true;
    charge.ReleaseAll();
    return;
  }

  // Spilled build side: partition the probe side symmetrically and join each
  // partition pair from disk. (The former in-memory / parallel-partition
  // branches here were unreachable: `spilled` is always true past the early
  // return above, so they were removed rather than revived — reviving them
  // without flushing the in-memory probe rows would lose data.)
  std::vector<SpillFile> left_spill(kReactiveSpillPartitions);
  RowPosition position;
  while (left_->Next(&row, &position)) {
    if (!EncodeJoinKey(row, left_cols_, &key)) { continue;
}
    left_spill[ReactivePartitionOf(key)].Append(row, position);
  }
  for (size_t i = 0; i < kReactiveSpillPartitions; ++i) {
    left_spill[i].FinishWriting();
    right_spill[i].FinishWriting();
  }
  for (size_t i = 0; i < kReactiveSpillPartitions; ++i) {
    auto left_part = left_spill[i].ReadAllPositioned();
    auto right_part = right_spill[i].ReadAllRows();
    QueryMemoryCharge part_charge;
    for (const auto& left : left_part) {
      part_charge.Add(EstimateRowBytes(left.first));
    }
    for (const Row& right : right_part) {
      part_charge.Add(EstimateRowBytes(right));
    }
    JoinPartition(left_cols_, right_cols_, left_part, right_part, &output_);
  }

  charge.ReleaseAll();
  ChargeOutput(&output_, &output_charge_);
  materialized_ = true;
}

void HashJoin::MaterializeHybrid() {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();

  // First pass estimate: stream right into spills with a provisional partition
  // count, keeping partition 0 resident when it fits.
  constexpr size_t kPartitions = 32;
  const auto partition_of = [](const std::string& key) {
    return std::hash<std::string>{}(key) % kPartitions;
  };

  std::vector<Row> resident_right;
  QueryMemoryCharge resident_charge;
  std::vector<SpillFile> right_spill(kPartitions);
  std::vector<SpillFile> left_spill(kPartitions);
  std::string key;

  Row row;
  RowPosition position;
  while (right_->Next(&row, nullptr)) {
    if (!EncodeJoinKey(row, right_cols_, &key)) { continue;
}
    const size_t part = partition_of(key);
    const size_t bytes = EstimateRowBytes(row);
    if (part == 0 && budget.CanReserve(bytes)) {
      resident_charge.Add(bytes);
      resident_right.push_back(std::move(row));
    } else {
      right_spill[part].Append(row);
    }
  }

  std::unordered_multimap<std::string, const Row*> buckets;
  buckets.reserve(resident_right.size());
  for (const Row& right_row : resident_right) {
    if (!EncodeJoinKey(right_row, right_cols_, &key)) { continue;
}
    buckets.emplace(key, &right_row);
  }

  while (left_->Next(&row, &position)) {
    if (!EncodeJoinKey(row, left_cols_, &key)) { continue;  // NULL key never matches
}
    const size_t part = partition_of(key);
    if (part == 0 && right_spill[0].Empty()) {
      // Pure resident partition: probe immediately.
      const auto [begin, end] = buckets.equal_range(key);
      for (auto match = begin; match != end; ++match) {
        output_.emplace_back(row + *match->second, position);
      }
    } else {
      // Non-resident partitions, or resident partition with overflow build
      // rows (join resident+overflow together after the probe scan).
      left_spill[part].Append(row, position);
    }
  }

  buckets.clear();
  if (!right_spill[0].Empty()) {
    // Fold the resident build into partition 0's spilled join.
    for (Row& right_row : resident_right) {
      right_spill[0].Append(right_row);
    }
  }
  resident_right.clear();
  resident_right.shrink_to_fit();
  resident_charge.ReleaseAll();

  for (size_t i = 0; i < kPartitions; ++i) {
    left_spill[i].FinishWriting();
    right_spill[i].FinishWriting();
  }

  for (size_t i = 0; i < kPartitions; ++i) {
    if (left_spill[i].Empty() && right_spill[i].Empty()) {
      continue;
    }
    auto left_part = left_spill[i].ReadAllPositioned();
    auto right_part = right_spill[i].ReadAllRows();
    QueryMemoryCharge part_charge;
    for (const auto& left : left_part) {
      part_charge.Add(EstimateRowBytes(left.first));
    }
    for (const Row& right : right_part) {
      part_charge.Add(EstimateRowBytes(right));
    }
    JoinPartition(left_cols_, right_cols_, left_part, right_part, &output_);
  }

  ChargeOutput(&output_, &output_charge_);
  materialized_ = true;
}

void HashJoin::Dump(std::ostream& o, int indent) const {
  std::stringstream ss;
  ss << "left: {";
  for (size_t i = 0; i < left_cols_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << left_cols_[i];
  }
  ss << "} right: {";
  for (size_t i = 0; i < right_cols_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << right_cols_[i];
  }
  ss << "}";
  if (mode_ == HashJoinMode::kHybrid) {
    o << "HybridHashJoin (" << worker_count_ << " workers): " << ss.str()
      << "\n"
      << Indent(indent + 2);
  } else if (pipelined_) {
    o << "PartitionedHashJoin (pipelined, " << worker_count_ << " workers): "
      << ss.str() << "\n"
      << Indent(indent + 2);
  } else {
    o << "PartitionedHashJoin (" << worker_count_ << " workers): " << ss.str()
      << "\n"
      << Indent(indent + 2);
  }
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_->Dump(o, indent + 2);
}

}  // namespace tinylamb
