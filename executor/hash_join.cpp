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
#include <exception>
#include <iterator>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/debug.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"

namespace tinylamb {
namespace {

constexpr size_t kReactiveSpillPartitions = 16;

size_t ReactivePartitionOf(const std::string& key) {
  return std::hash<std::string>{}(key) % kReactiveSpillPartitions;
}

void JoinPartition(
    const std::vector<slot_t>& left_cols, const std::vector<slot_t>& right_cols,
    const std::vector<std::pair<Row, RowPosition>>& left_rows,
    const std::vector<Row>& right_rows,
    std::vector<std::pair<Row, RowPosition>>* output) {
  std::unordered_multimap<std::string, const Row*> buckets;
  buckets.reserve(right_rows.size());
  for (const Row& right_row : right_rows) {
    buckets.emplace(right_row.Extract(right_cols).EncodeMemcomparableFormat(),
                    &right_row);
  }
  for (const auto& left : left_rows) {
    const std::string key =
        left.first.Extract(left_cols).EncodeMemcomparableFormat();
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

bool HashJoin::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) Materialize();
  if (output_offset_ == output_.size()) return false;
  *dst = output_[output_offset_].first;
  if (rp) *rp = output_[output_offset_].second;
  ++output_offset_;
  return true;
}

size_t HashJoin::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (!materialized_) Materialize();
  while (output_offset_ < output_.size() &&
         destination->Size() < max_rows) {
    destination->Append(output_[output_offset_].first,
                        output_[output_offset_].second);
    ++output_offset_;
  }
  return destination->Size();
}

void HashJoin::Materialize() {
  if (mode_ == HashJoinMode::kHybrid) {
    MaterializeHybrid();
  } else {
    MaterializeInMemory();
  }
}

void HashJoin::MaterializeInMemory() {
  using PositionedRow = std::pair<Row, RowPosition>;
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  QueryMemoryCharge charge;

  std::vector<PositionedRow> left_rows;
  std::vector<Row> right_rows;
  std::vector<SpillFile> left_spill(kReactiveSpillPartitions);
  std::vector<SpillFile> right_spill(kReactiveSpillPartitions);
  bool spilled = false;

  Row row;
  RowPosition position;
  while (left_->Next(&row, &position)) {
    const size_t bytes = EstimateRowBytes(row) + sizeof(RowPosition);
    if (!spilled && !budget.CanReserve(bytes)) {
      spilled = true;
      for (const PositionedRow& existing : left_rows) {
        const std::string key =
            existing.first.Extract(left_cols_).EncodeMemcomparableFormat();
        left_spill[ReactivePartitionOf(key)].Append(existing.first,
                                                    existing.second);
      }
      charge.ReleaseAll();
      left_rows.clear();
      left_rows.shrink_to_fit();
    }
    if (spilled) {
      const std::string key =
          row.Extract(left_cols_).EncodeMemcomparableFormat();
      left_spill[ReactivePartitionOf(key)].Append(row, position);
    } else {
      charge.Add(bytes);
      left_rows.emplace_back(std::move(row), position);
    }
  }

  while (right_->Next(&row, nullptr)) {
    const size_t bytes = EstimateRowBytes(row);
    if (!spilled && !budget.CanReserve(bytes)) {
      spilled = true;
      for (const PositionedRow& existing : left_rows) {
        const std::string key =
            existing.first.Extract(left_cols_).EncodeMemcomparableFormat();
        left_spill[ReactivePartitionOf(key)].Append(existing.first,
                                                    existing.second);
      }
      for (const Row& existing : right_rows) {
        const std::string key =
            existing.Extract(right_cols_).EncodeMemcomparableFormat();
        right_spill[ReactivePartitionOf(key)].Append(existing);
      }
      charge.ReleaseAll();
      left_rows.clear();
      left_rows.shrink_to_fit();
      right_rows.clear();
      right_rows.shrink_to_fit();
    }
    if (spilled) {
      const std::string key =
          row.Extract(right_cols_).EncodeMemcomparableFormat();
      right_spill[ReactivePartitionOf(key)].Append(row);
    } else {
      charge.Add(bytes);
      right_rows.push_back(std::move(row));
    }
  }

  if (!spilled) {
    const size_t partitions = std::min(
        worker_count_,
        std::max<size_t>(1, left_rows.size() + right_rows.size()));
    std::vector<std::vector<PositionedRow>> left_partitions(partitions);
    std::vector<std::vector<Row>> right_partitions(partitions);
    const std::hash<std::string> hash;
    for (PositionedRow& left : left_rows) {
      std::string key =
          left.first.Extract(left_cols_).EncodeMemcomparableFormat();
      left_partitions[hash(key) % partitions].push_back(std::move(left));
    }
    for (Row& right_row : right_rows) {
      std::string key =
          right_row.Extract(right_cols_).EncodeMemcomparableFormat();
      right_partitions[hash(key) % partitions].push_back(std::move(right_row));
    }
    left_rows.clear();
    right_rows.clear();

    std::vector<std::vector<PositionedRow>> partition_output(partitions);
    std::exception_ptr error;
    std::mutex error_mutex;
    std::vector<std::jthread> workers;
    workers.reserve(partitions);
    for (size_t partition = 0; partition < partitions; ++partition) {
      workers.emplace_back([&, partition] {
        try {
          JoinPartition(left_cols_, right_cols_, left_partitions[partition],
                        right_partitions[partition],
                        &partition_output[partition]);
        } catch (...) {
          std::scoped_lock lock(error_mutex);
          if (!error) error = std::current_exception();
        }
      });
    }
    workers.clear();
    if (error) std::rethrow_exception(error);
    size_t output_size = 0;
    for (const auto& partition : partition_output) {
      output_size += partition.size();
    }
    output_.reserve(output_size);
    for (auto& partition : partition_output) {
      std::move(partition.begin(), partition.end(),
                std::back_inserter(output_));
    }
  } else {
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

  Row row;
  RowPosition position;
  while (right_->Next(&row, nullptr)) {
    const std::string key =
        row.Extract(right_cols_).EncodeMemcomparableFormat();
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
    buckets.emplace(right_row.Extract(right_cols_).EncodeMemcomparableFormat(),
                    &right_row);
  }

  while (left_->Next(&row, &position)) {
    const std::string key =
        row.Extract(left_cols_).EncodeMemcomparableFormat();
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
